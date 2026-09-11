package com.fdsv2.feedback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fdsv2.decision.FraudDecision;
import com.fdsv2.modelclient.RawFeatureStep;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * CP6 — CP5 판정이 끝날 때마다 호출되어, 라벨이 지연 공개될 때까지 Redis에 대기시켜 둔다
 * ({@link FraudDecisionEventListener}에서 {@code decide()} 성공 직후 호출 — 실패 격리는 호출부
 * 책임, 이 클래스도 방어적으로 예외를 삼킨다).
 *
 * <p>이 시점에 {@link SimulatedLabelHeuristic}으로 라벨을 이미 계산해 {@link PendingDecisionRecord}에
 * 담아둔다({@code labelSource} 상수가 지금은 {@code SIMULATED} 하나뿐이다 — 실제 라벨 소스(예: 이의제기
 * 시스템 연동)가 추가되면 이 자리에서 분기해야 한다).
 */
@Slf4j
@Component
public class FeedbackDecisionRecorder {

    private static final String LABEL_SOURCE_SIMULATED = "SIMULATED";

    private final StringRedisTemplate redisTemplate;
    private final FeedbackKeyBuilder keyBuilder;
    private final SimulatedLabelHeuristic labelHeuristic;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final long minDelaySeconds;
    private final long maxDelaySeconds;
    private final long pendingTtlSeconds;

    public FeedbackDecisionRecorder(
            StringRedisTemplate redisTemplate,
            FeedbackKeyBuilder keyBuilder,
            SimulatedLabelHeuristic labelHeuristic,
            MeterRegistry meterRegistry,
            @Value("${fds.feedback.label-simulator.min-delay-seconds:300}") long minDelaySeconds,
            @Value("${fds.feedback.label-simulator.max-delay-seconds:1800}") long maxDelaySeconds,
            @Value("${fds.feedback.pending-ttl-seconds:5400}") long pendingTtlSeconds) {
        this.redisTemplate = redisTemplate;
        this.keyBuilder = keyBuilder;
        this.labelHeuristic = labelHeuristic;
        this.meterRegistry = meterRegistry;
        this.minDelaySeconds = minDelaySeconds;
        this.maxDelaySeconds = maxDelaySeconds;
        this.pendingTtlSeconds = pendingTtlSeconds;
    }

    public void record(String accountId, FraudDecision decision, List<RawFeatureStep> sequence) {
        try {
            doRecord(accountId, decision, sequence);
        } catch (Exception e) {
            log.warn("CP6 판정 기록 실패, 건너뜀(판정 자체는 이미 성공함): accountId={}, cause={}",
                    accountId, e.toString());
        }
    }

    private void doRecord(String accountId, FraudDecision decision, List<RawFeatureStep> sequence) throws Exception {
        String decisionId = UUID.randomUUID().toString();
        List<FeedbackTransactionStep> transactions =
                sequence.stream().map(FeedbackTransactionStep::from).toList();
        int label = labelHeuristic.label(sequence);
        long delaySeconds = randomDelaySeconds();
        Instant scheduledRevealAt = Instant.now().plusSeconds(delaySeconds);

        PendingDecisionRecord pending = new PendingDecisionRecord(
                decisionId, accountId, decision.decidedAt(), decision.action(), decision.combinedScore(),
                decision.modelProbability(), decision.modelSource(), decision.ruleScore(),
                decision.triggeredRules(), transactions, label, LABEL_SOURCE_SIMULATED, delaySeconds,
                scheduledRevealAt);

        String json = objectMapper.writeValueAsString(pending);
        redisTemplate.opsForValue().set(keyBuilder.pendingKey(decisionId), json, Duration.ofSeconds(pendingTtlSeconds));
        redisTemplate.opsForZSet().add(keyBuilder.pendingIndexKey(), decisionId, scheduledRevealAt.getEpochSecond());
        meterRegistry.counter("fds.feedback.decision.recorded.count").increment();
    }

    private long randomDelaySeconds() {
        if (maxDelaySeconds <= minDelaySeconds) {
            return minDelaySeconds;
        }
        return ThreadLocalRandom.current().nextLong(minDelaySeconds, maxDelaySeconds + 1);
    }
}
