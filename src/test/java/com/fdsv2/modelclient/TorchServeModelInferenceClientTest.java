package com.fdsv2.modelclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * TorchServeHttpCaller를 목으로 대체해서 "성공/실패/서킷오픈" 세 시나리오만 검증한다 — 진짜
 * HTTP 서버나 TorchServe는 필요 없다 (TorchServeHttpCaller 인터페이스 javadoc 참고).
 * 실제 TorchServe 대상 e2e 검증은 세션 로그 참고.
 */
@ExtendWith(MockitoExtension.class)
class TorchServeModelInferenceClientTest {

    @Mock
    private AccountRecentSequenceReader sequenceReader;

    @Mock
    private RuleBasedFallbackScorer fallbackScorer;

    @Mock
    private TorchServeHttpCaller httpCaller;

    private CircuitBreaker circuitBreaker;
    private TorchServeModelInferenceClient client;

    @BeforeEach
    void setUp() {
        // 실패율 계산에 최소 호출 수를 요구하지 않게 해서(minimumNumberOfCalls=1), 테스트마다
        // 새 CircuitBreaker 인스턴스로 결정적인 상태에서 시작한다.
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(2)
                .minimumNumberOfCalls(1)
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .build();
        circuitBreaker = CircuitBreaker.of("test-torchserve", config);
        client = new TorchServeModelInferenceClient(sequenceReader, fallbackScorer, httpCaller, circuitBreaker);
    }

    @Test
    void 정상_호출이면_모델_점수를_그대로_반환한다() {
        List<RawFeatureStep> steps = List.of(
                new RawFeatureStep("acc-1", 1, 1.0, null, false, "GROCERY"));
        when(sequenceReader.readRecentSteps("acc-1")).thenReturn(steps);
        when(httpCaller.call(anyString()))
                .thenReturn("{\"accountId\":\"acc-1\",\"fraudProbability\":0.0731}");

        FraudScore score = client.predict("acc-1");

        assertThat(score.accountId()).isEqualTo("acc-1");
        assertThat(score.fraudProbability()).isEqualTo(0.0731);
        assertThat(score.source()).isEqualTo(FraudScore.SOURCE_MODEL);
    }

    @Test
    void TorchServe_호출이_실패하면_규칙_기반_폴백으로_전환한다() {
        RawFeatureStep latest = new RawFeatureStep("acc-2", 1, 7.0, 5L, true, "CASH_ADVANCE");
        when(sequenceReader.readRecentSteps("acc-2")).thenReturn(List.of(latest));
        when(httpCaller.call(anyString())).thenThrow(new RuntimeException("connection refused"));
        when(fallbackScorer.score(latest)).thenReturn(0.9);

        FraudScore score = client.predict("acc-2");

        assertThat(score.fraudProbability()).isEqualTo(0.9);
        assertThat(score.source()).isEqualTo(FraudScore.SOURCE_FALLBACK);
        verify(fallbackScorer).score(latest);
    }

    @Test
    void 계좌_이력이_없어도_폴백_스코어러에_null을_넘겨_처리한다() {
        when(sequenceReader.readRecentSteps("acc-none")).thenReturn(List.of());
        when(httpCaller.call(anyString())).thenThrow(new RuntimeException("boom"));
        when(fallbackScorer.score(null)).thenReturn(0.5);

        FraudScore score = client.predict("acc-none");

        assertThat(score.fraudProbability()).isEqualTo(0.5);
        assertThat(score.source()).isEqualTo(FraudScore.SOURCE_FALLBACK);
    }

    @Test
    void 서킷이_열리면_TorchServe를_호출하지_않고_바로_폴백한다() {
        // slidingWindowSize=2, minimumNumberOfCalls=1, failureRateThreshold=50 이므로
        // 실패 1건만으로도 OPEN으로 전환된다.
        when(sequenceReader.readRecentSteps(anyString())).thenReturn(List.of());
        when(httpCaller.call(anyString())).thenThrow(new RuntimeException("boom"));
        when(fallbackScorer.score(null)).thenReturn(0.5);

        client.predict("acc-3"); // 이 호출로 서킷이 OPEN 전환됨
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        client.predict("acc-3"); // 서킷이 열려있으니 httpCaller가 또 호출되면 안 됨

        verify(httpCaller, org.mockito.Mockito.times(1)).call(anyString());
    }
}
