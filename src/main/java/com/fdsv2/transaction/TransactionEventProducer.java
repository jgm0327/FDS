package com.fdsv2.transaction;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * transaction-events 토픽 발행 담당.
 *
 * accountId를 메시지 key로 명시하는 것이 파티셔닝 전략의 핵심 전제조건이다 — 같은 계좌의
 * 이벤트가 항상 같은 파티션(같은 Kafka Streams 태스크)으로 가야 시퀀스 순서가 보장된다
 * (docs/ARCHITECTURE.md 1번 참고).
 *
 * 프로듀서 신뢰성 설정(idempotence, acks=all, max.in.flight=5)은 application.yml에서 관리하며,
 * 재시도 상황에서도 파티션 내 순서가 깨지지 않도록 하는 Kafka 공식 권장 조합이다
 * (docs/BACKEND.md 핵심 설계 결정 2번 참고).
 *
 * <p>(backend/kafka-salting) <b>Salting</b> — CP1 세션(hot partition k6 재현)에서 실측으로 확인한
 * 문제: 초고빈도 계좌(PG사 등) 하나가 파티션 하나에 트래픽을 몰아주면, 그 계좌를 담당하는 Kafka
 * Streams 태스크 하나가 처리량 한계에 부딪힌다 — 컨슈머 인스턴스를 늘려도 안 풀린다(파티션 1개는
 * 그룹 내 스레드 1개만 처리 가능). 진짜 해법은 그 계좌의 트래픽을 여러 파티션(서브샤드)으로 쪼개
 * 병렬 처리한 뒤 다시 계좌 단위로 재집계하는 것 — {@link AccountShardKey}로 메시지 키를
 * "accountId#shardIndex" 형식으로 만든다.
 *
 * <p>{@code fds.kafka.transaction-events.salting.high-traffic-account-ids}에 등록되지 않은
 * 일반 계좌는 항상 shardIndex=0으로 고정된다 — "accountId#0"이 항상 같은 파티션으로 가므로 순서
 * 보장에 변화가 없다. 등록된 계좌만 shardIndex를 0~(shard-count-1) 중 무작위로 분산해서 여러
 * 파티션/Streams 태스크가 나눠 처리하게 한다. 재집계 로직은
 * {@code com.fdsv2.sequence.AccountActivityMergeProcessor} 참고.
 */
@Slf4j
@Component
public class TransactionEventProducer {

    /**
     * CP1 "프로듀서 발행 latency (p95/p99)" 지표용 (docs/PERFORMANCE_MEASUREMENT.md 참고).
     *
     * Kafka 클라이언트 자체가 노출하는 producer 메트릭(kafka_producer_request_latency_avg 등)은
     * avg/max만 제공하고 퍼센타일 히스토그램이 없어서, send() 완료까지 걸린 시간을 앱에서 직접
     * Timer로 측정한다. publishPercentileHistogram()으로 버킷 히스토그램만 노출하고, p50/p95/p99은
     * Grafana에서 histogram_quantile()로 계산한다 — 클라이언트 사이드 publishPercentiles()도
     * 시도해봤으나 이 Micrometer/Prometheus 조합에서는 quantile 라벨이 달린 요약 시계열이 실제로
     * 노출되지 않는 걸 확인해서(actuator/prometheus에 quantile 라인이 전혀 없음), 여러 인스턴스로
     * 확장해도 정확히 합산되는 histogram_quantile 방식으로 통일했다.
     */
    private static final String PUBLISH_TIMER_NAME = "fds.transaction.publish.duration";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final String topicName;
    private final Set<String> highTrafficAccountIds;
    private final int shardCount;

    public TransactionEventProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${fds.kafka.transaction-events.topic-name}") String topicName,
            @Value("${fds.kafka.transaction-events.salting.high-traffic-account-ids:}")
                    String highTrafficAccountIdsCsv,
            @Value("${fds.kafka.transaction-events.salting.shard-count:8}") int shardCount) {
        // 코드 리뷰 지적: shard-count가 0 이하로 설정되면(오타, "0으로 끄려는" 시도 등)
        // ThreadLocalRandom.nextInt(shardCount)가 IllegalArgumentException을 던지는데, 이게
        // publish() 호출 시점(요청 처리 중)에야 터져서 모든 거래 발행이 500으로 실패한다 —
        // 애플리케이션 기동 시점에 미리 fail-fast로 막는다.
        if (shardCount <= 0) {
            throw new IllegalArgumentException(
                    "fds.kafka.transaction-events.salting.shard-count는 1 이상이어야 한다: " + shardCount);
        }
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.topicName = topicName;
        this.highTrafficAccountIds = parseCsv(highTrafficAccountIdsCsv);
        this.shardCount = shardCount;
    }

    public void publish(TransactionEvent event) {
        Timer.Sample sample = Timer.start(meterRegistry);
        String key = AccountShardKey.format(event.accountId(), shardIndexFor(event.accountId()));

        kafkaTemplate.send(topicName, key, event)
                .whenComplete((result, ex) -> {
                    sample.stop(Timer.builder(PUBLISH_TIMER_NAME)
                            .description("accountId 파티션 키로 send() 호출 후 브로커 ack까지 걸린 시간")
                            .tag("outcome", ex == null ? "success" : "failure")
                            .publishPercentileHistogram()
                            .register(meterRegistry));

                    if (ex != null) {
                        log.error("거래 이벤트 발행 실패: accountId={}, transactionId={}",
                                event.accountId(), event.transactionId(), ex);
                        return;
                    }
                    var metadata = result.getRecordMetadata();
                    log.info("거래 이벤트 발행 성공: accountId={}, transactionId={}, partition={}, offset={}",
                            event.accountId(), event.transactionId(), metadata.partition(), metadata.offset());
                });
    }

    /** 고빈도 계좌만 샤드를 무작위 분산 — 일반 계좌는 항상 0(파티셔닝 불변). */
    private int shardIndexFor(String accountId) {
        return highTrafficAccountIds.contains(accountId)
                ? ThreadLocalRandom.current().nextInt(shardCount)
                : 0;
    }

    private static Set<String> parseCsv(String csv) {
        if (csv == null || csv.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }
}
