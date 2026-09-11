package com.fdsv2.feedback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fdsv2.decision.Action;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

@ExtendWith(MockitoExtension.class)
class FeedbackLabelSchedulerTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private ZSetOperations<String, String> zSetOperations;

    @Mock
    private FeedbackDatasetWriter datasetWriter;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private FeedbackKeyBuilder keyBuilder;
    private SimpleMeterRegistry meterRegistry;
    private FeedbackLabelScheduler scheduler;

    @BeforeEach
    void setUp() {
        keyBuilder = new FeedbackKeyBuilder("feedback:pending:", "feedback:pending:index");
        meterRegistry = new SimpleMeterRegistry();
        scheduler = new FeedbackLabelScheduler(redisTemplate, keyBuilder, datasetWriter, meterRegistry);
    }

    private PendingDecisionRecord pending(String decisionId, int label) {
        return new PendingDecisionRecord(
                decisionId, "acc-1", Instant.now(), Action.BLOCK, 0.9, 0.9, "MODEL", 0.0,
                List.of("HARD_BLOCK"),
                List.of(new FeedbackTransactionStep(1.0, 300.0, false, "GROCERY")),
                label, "SIMULATED", 900, Instant.now());
    }

    @Test
    void 만기된_항목이_없으면_아무것도_하지_않는다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.rangeByScore(eq("feedback:pending:index"), anyDouble(), anyDouble())).thenReturn(Set.of());

        scheduler.revealDueLabels();

        verify(datasetWriter, never()).append(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void 만기된_항목을_데이터셋에_기록하고_Redis에서_제거한다() throws Exception {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(zSetOperations.rangeByScore(eq("feedback:pending:index"), anyDouble(), anyDouble()))
                .thenReturn(Set.of("decision-1"));
        String json = objectMapper.writeValueAsString(pending("decision-1", 1));
        when(valueOperations.get("feedback:pending:decision-1")).thenReturn(json);

        scheduler.revealDueLabels();

        verify(datasetWriter).append(org.mockito.ArgumentMatchers.argThat(
                record -> record.decisionId().equals("decision-1") && record.label() == 1));
        verify(redisTemplate).delete("feedback:pending:decision-1");
        verify(zSetOperations).remove("feedback:pending:index", "decision-1");
        assertThat(meterRegistry.get("fds.feedback.label.count").tag("label", "1").counter().count()).isEqualTo(1.0);
    }

    @Test
    void pending_레코드가_이미_사라졌으면_인덱스만_정리한다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(zSetOperations.rangeByScore(eq("feedback:pending:index"), anyDouble(), anyDouble()))
                .thenReturn(Set.of("decision-gone"));
        when(valueOperations.get("feedback:pending:decision-gone")).thenReturn(null);

        scheduler.revealDueLabels();

        verify(datasetWriter, never()).append(org.mockito.ArgumentMatchers.any());
        verify(redisTemplate).delete("feedback:pending:decision-gone");
        verify(zSetOperations).remove("feedback:pending:index", "decision-gone");
    }

    @Test
    void 손상된_레코드는_예외를_던지지_않고_버린다() {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(zSetOperations.rangeByScore(eq("feedback:pending:index"), anyDouble(), anyDouble()))
                .thenReturn(Set.of("decision-broken"));
        when(valueOperations.get("feedback:pending:decision-broken")).thenReturn("not-json");

        assertThatCode(() -> scheduler.revealDueLabels()).doesNotThrowAnyException();

        verify(datasetWriter, never()).append(org.mockito.ArgumentMatchers.any());
        verify(redisTemplate).delete("feedback:pending:decision-broken");
        verify(zSetOperations).remove("feedback:pending:index", "decision-broken");
    }

    @Test
    void 데이터셋_쓰기가_실패해도_예외를_던지지_않고_Redis에서는_제거한다() throws Exception {
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(zSetOperations.rangeByScore(eq("feedback:pending:index"), anyDouble(), anyDouble()))
                .thenReturn(Set.of("decision-1"));
        String json = objectMapper.writeValueAsString(pending("decision-1", 0));
        when(valueOperations.get("feedback:pending:decision-1")).thenReturn(json);
        org.mockito.Mockito.doThrow(new RuntimeException("disk full"))
                .when(datasetWriter).append(org.mockito.ArgumentMatchers.any());

        assertThatCode(() -> scheduler.revealDueLabels()).doesNotThrowAnyException();

        verify(redisTemplate).delete("feedback:pending:decision-1");
        verify(zSetOperations).remove("feedback:pending:index", "decision-1");
    }
}
