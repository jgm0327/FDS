package com.fdsv2.feedback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * CP6 — 라벨 공개 예정 시각이 지난 {@link PendingDecisionRecord}를 찾아
 * {@link FeedbackDatasetRecord}로 변환해 {@link FeedbackDatasetWriter}에 기록한다("라벨 지연
 * 도착"의 실제 구현체).
 *
 * <p>라벨 자체는 {@link FeedbackDecisionRecorder}가 기록 시점에 이미 계산해뒀다 — 여기서는 다시
 * 채점하지 않고, 정렬집합({@code feedback:pending:index}, score=공개 예정 시각 epoch초)에서
 * {@code ZRANGEBYSCORE(-inf, now)}로 "만기된" decisionId만 뽑아 공개(파일로 옮김)하고 Redis에서
 * 지운다. {@code KEYS}로 전체 스캔하지 않는 표준 패턴.
 *
 * <p><b>손상된 레코드는 재시도하지 않고 버린다</b> — pending 레코드 파싱이나 데이터셋 파일 쓰기가
 * 실패해도 그 decisionId는 즉시 Redis에서 제거한다(finally). 그러지 않으면 손상된 레코드 하나가
 * 매 폴링마다 계속 실패 로그를 남기며 영원히 재시도되는 "poison pill" 상태에 빠진다 — 이번
 * 토이 프로젝트 범위에서는 완전한 DLQ(dead-letter queue)보다 "실패하면 버리고 계속 진행"이 더
 * 합리적인 트레이드오프라고 판단했다.
 */
@Slf4j
@Component
public class FeedbackLabelScheduler {

    private final StringRedisTemplate redisTemplate;
    private final FeedbackKeyBuilder keyBuilder;
    private final FeedbackDatasetWriter datasetWriter;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public FeedbackLabelScheduler(
            StringRedisTemplate redisTemplate,
            FeedbackKeyBuilder keyBuilder,
            FeedbackDatasetWriter datasetWriter,
            MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.keyBuilder = keyBuilder;
        this.datasetWriter = datasetWriter;
        this.meterRegistry = meterRegistry;
    }

    @Scheduled(fixedDelayString = "${fds.feedback.label-simulator.poll-interval-ms:30000}")
    public void revealDueLabels() {
        double nowEpochSec = Instant.now().getEpochSecond();
        Set<String> dueDecisionIds = redisTemplate.opsForZSet()
                .rangeByScore(keyBuilder.pendingIndexKey(), Double.NEGATIVE_INFINITY, nowEpochSec);
        if (dueDecisionIds == null || dueDecisionIds.isEmpty()) {
            return;
        }
        for (String decisionId : dueDecisionIds) {
            revealOne(decisionId);
        }
    }

    private void revealOne(String decisionId) {
        String pendingKey = keyBuilder.pendingKey(decisionId);
        try {
            String json = redisTemplate.opsForValue().get(pendingKey);
            if (json == null) {
                // TTL 만료 등으로 이미 사라짐 — finally에서 인덱스만 정리하면 된다.
                return;
            }
            PendingDecisionRecord pending = objectMapper.readValue(json, PendingDecisionRecord.class);
            FeedbackDatasetRecord record = toDatasetRecord(pending);
            datasetWriter.append(record);
            meterRegistry.counter("fds.feedback.label.count",
                    "label", String.valueOf(pending.label()), "source", pending.labelSource()).increment();
            meterRegistry.timer("fds.feedback.label.delay.seconds")
                    .record(Duration.ofSeconds(pending.labelDelaySeconds()));
        } catch (Exception e) {
            log.warn("CP6 라벨 공개 실패, pending 레코드는 버림(재시도 안 함): decisionId={}, cause={}",
                    decisionId, e.toString());
        } finally {
            redisTemplate.delete(pendingKey);
            redisTemplate.opsForZSet().remove(keyBuilder.pendingIndexKey(), decisionId);
        }
    }

    private FeedbackDatasetRecord toDatasetRecord(PendingDecisionRecord pending) {
        return new FeedbackDatasetRecord(
                pending.decisionId(), pending.accountId(), pending.decidedAt(), pending.action(),
                pending.combinedScore(), pending.modelProbability(), pending.modelSource(), pending.ruleScore(),
                pending.triggeredRules(), pending.transactions(), pending.label(), pending.labelSource(),
                pending.labelDelaySeconds(), Instant.now());
    }
}
