package com.fdsv2.feedback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fdsv2.decision.Action;
import com.fdsv2.decision.FraudDecision;
import com.fdsv2.modelclient.RawFeatureStep;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

/**
 * 실제 Redis 없이 StringRedisTemplate을 모킹해서 검증한다 —
 * AccountFeatureStoreSinkListenerTest와 동일한 방식.
 */
@ExtendWith(MockitoExtension.class)
class FeedbackDecisionRecorderTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    private FeedbackKeyBuilder keyBuilder;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        keyBuilder = new FeedbackKeyBuilder("feedback:pending:", "feedback:pending:index");
        meterRegistry = new SimpleMeterRegistry();
    }

    private FeedbackDecisionRecorder recorder(long minDelay, long maxDelay, long ttl, int fixedLabel) {
        SimulatedLabelHeuristic heuristic = new SimulatedLabelHeuristic(0.0, new Random()) {
            @Override
            public int label(List<RawFeatureStep> sequence) {
                return fixedLabel;
            }
        };
        return new FeedbackDecisionRecorder(
                redisTemplate, keyBuilder, heuristic, meterRegistry, minDelay, maxDelay, ttl);
    }

    private FraudDecision decision() {
        return new FraudDecision("acc-1", Action.BLOCK, 0.9, 0.9, "MODEL", 0.0, List.of("HARD_BLOCK"), Instant.now());
    }

    @Test
    void 판정을_Redis에_TTL과_함께_저장하고_인덱스에도_추가한다() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        List<RawFeatureStep> sequence = List.of(new RawFeatureStep("acc-1", 1, 1.0, 300L, false, "GROCERY"));

        recorder(300, 300, 900, 1).record("acc-1", decision(), sequence);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(valueOperations).set(keyCaptor.capture(), anyString(), eq(Duration.ofSeconds(900)));
        assertThat(keyCaptor.getValue()).startsWith("feedback:pending:");
        verify(zSetOperations).add(eq("feedback:pending:index"), eq(keyCaptor.getValue()
                .substring("feedback:pending:".length())), anyDouble());
    }

    @Test
    void 지연은_min과_max_사이의_epoch초로_인덱스에_기록된다() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        List<RawFeatureStep> sequence = List.of(new RawFeatureStep("acc-1", 1, 1.0, 300L, false, "GROCERY"));
        long before = Instant.now().getEpochSecond();

        recorder(300, 1800, 5400, 0).record("acc-1", decision(), sequence);

        ArgumentCaptor<Double> scoreCaptor = ArgumentCaptor.forClass(Double.class);
        verify(zSetOperations).add(eq("feedback:pending:index"), anyString(), scoreCaptor.capture());
        double score = scoreCaptor.getValue();
        assertThat(score).isBetween((double) (before + 300), (double) (before + 1800 + 5));
    }

    @Test
    void 지표_fds_feedback_decision_recorded_count가_증가한다() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        List<RawFeatureStep> sequence = List.of(new RawFeatureStep("acc-1", 1, 1.0, 300L, false, "GROCERY"));

        recorder(300, 300, 900, 0).record("acc-1", decision(), sequence);

        assertThat(meterRegistry.get("fds.feedback.decision.recorded.count").counter().count()).isEqualTo(1.0);
    }

    @Test
    void Redis_쓰기가_실패해도_예외를_밖으로_던지지_않는다() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        org.mockito.Mockito.doThrow(new RuntimeException("Redis connection refused"))
                .when(valueOperations).set(any(), any(), any(Duration.class));
        List<RawFeatureStep> sequence = List.of(new RawFeatureStep("acc-1", 1, 1.0, 300L, false, "GROCERY"));

        assertThatCode(() -> recorder(300, 300, 900, 0).record("acc-1", decision(), sequence))
                .doesNotThrowAnyException();
    }
}
