package com.fdsv2.transaction;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
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
 */
@Slf4j
@Component
@RequiredArgsConstructor
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

    @Value("${fds.kafka.transaction-events.topic-name}")
    private String topicName;

    public void publish(TransactionEvent event) {
        Timer.Sample sample = Timer.start(meterRegistry);

        kafkaTemplate.send(topicName, event.accountId(), event)
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
}
