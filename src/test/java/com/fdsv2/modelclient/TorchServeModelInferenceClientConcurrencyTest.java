package com.fdsv2.modelclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * (backend/model-client-concurrency-fix) k6 10 VU 동시 부하로 실측한 문제 — TorchServe 워커
 * 수보다 많은 동시 요청이 그대로 흘러가면 큐잉 대기가 timeout-ms를 넘겨 "가짜 실패"로 Circuit
 * Breaker에 잡힌다(ModelClientConfig 클래스 javadoc 참고) — 를 회귀 방지하는 테스트.
 *
 * maxConcurrentCalls=1인 Bulkhead 뒤에서, 이미 진행 중인 호출이 하나 있을 때 두 번째 동시 호출이
 * TorchServe까지 가지 않고 즉시 폴백으로 빠지는지, 그리고 그 거절이 CircuitBreaker의 성공/실패
 * 통계에 전혀 안 잡히는지를 확인한다.
 */
@ExtendWith(MockitoExtension.class)
class TorchServeModelInferenceClientConcurrencyTest {

    @Mock
    private AccountRecentSequenceReader sequenceReader;

    @Mock
    private RuleBasedFallbackScorer fallbackScorer;

    @Mock
    private TorchServeHttpCaller httpCaller;

    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void 동시_호출이_bulkhead_한도를_넘으면_TorchServe를_호출하지_않고_즉시_폴백한다() throws Exception {
        CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(10)
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .build();
        CircuitBreaker circuitBreaker = CircuitBreaker.of("test-torchserve", cbConfig);
        Bulkhead bulkhead = Bulkhead.of(
                "test-torchserve",
                BulkheadConfig.custom().maxConcurrentCalls(1).maxWaitDuration(Duration.ZERO).build());
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        TorchServeModelInferenceClient client = new TorchServeModelInferenceClient(
                sequenceReader, fallbackScorer, httpCaller, circuitBreaker, bulkhead, meterRegistry);

        when(sequenceReader.readRecentSteps(anyString())).thenReturn(List.of());
        when(fallbackScorer.score(null)).thenReturn(0.5);

        // 첫 호출이 bulkhead 허가를 잡은 채로 응답을 안 주도록(진행 중 상태 유지) 블로킹시킨다.
        CountDownLatch firstCallStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstCall = new CountDownLatch(1);
        when(httpCaller.call(anyString())).thenAnswer(invocation -> {
            firstCallStarted.countDown();
            releaseFirstCall.await(5, TimeUnit.SECONDS);
            return "{\"accountId\":\"acc-1\",\"fraudProbability\":0.1}";
        });

        Future<FraudScore> firstCall = executor.submit(() -> client.predict("acc-1"));
        assertThat(firstCallStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // 첫 호출이 아직 bulkhead 허가를 쥐고 있는 동안 두 번째 호출 — 즉시 거절되어야 한다.
        FraudScore secondScore = client.predict("acc-1");

        releaseFirstCall.countDown();
        FraudScore firstScore = firstCall.get(5, TimeUnit.SECONDS);

        assertThat(secondScore.source()).isEqualTo(FraudScore.SOURCE_FALLBACK);
        assertThat(firstScore.source()).isEqualTo(FraudScore.SOURCE_MODEL);
        // httpCaller는 첫 호출 한 번만 실제로 탔다 — 두 번째는 bulkhead가 걸러서 TorchServe 근처도
        // 못 갔다.
        verify(httpCaller, times(1)).call(anyString());
        // bulkhead 거절은 CircuitBreaker.executeSupplier 자체를 호출하지 않으므로, CircuitBreaker
        // 통계에는 성공 1건만 잡히고 실패는 0건이어야 한다(ModelClientConfig 클래스 javadoc 참고).
        assertThat(circuitBreaker.getMetrics().getNumberOfSuccessfulCalls()).isEqualTo(1);
        assertThat(circuitBreaker.getMetrics().getNumberOfFailedCalls()).isEqualTo(0);
    }
}
