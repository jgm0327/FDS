package com.fdsv2.modelclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * CP4 모델 서빙 호출의 실제 구현체 — Redis에서 최근 거래 시퀀스를 읽고, TorchServe를 Circuit
 * Breaker로 감싸 호출하며, 실패 시 규칙 기반 폴백으로 전환한다 (docs/ARCHITECTURE.md 4번).
 *
 * 실패로 간주하는 범위: HTTP 타임아웃/연결 실패/TorchServe의 5xx 응답/JSON 파싱 실패를 전부
 * 뭉뚱그려 "이 경로는 지금 못 믿는다"로 처리한다 — 원인별로 다르게 반응할 필요가 이번 범위에서는
 * 없고, 어차피 전부 같은 폴백(규칙 기반 스코어)으로 수렴한다. 원인별 세분화는 CP4 성능 측정
 * (Prometheus 연동, 다음 세션)에서 필요해지면 그때 나눈다.
 *
 * <p>(backend/model-client-observability) {@code fds.fraud.score.count} 카운터(태그
 * source=MODEL|FALLBACK)는 CircuitBreaker 상태 지표만으로는 알 수 없는 "실제로 호출부에
 * 몇 번 폴백이 나갔는지"를 직접 센다 — docs/PERFORMANCE_MEASUREMENT.md CP4 "타임아웃/서킷브레이커
 * 오픈 발생률" 판단 근거. {@code fds.fallback.scorer.latency} 타이머는 같은 표의 "폴백 발생 시
 * 규칙 기반 스코어 응답 latency" 행에 대응 — 자체 계측이라고 명시된 항목이라 Resilience4j 지표가
 * 아니라 직접 Timer로 감쌌다.
 */
@Slf4j
@Component
public class TorchServeModelInferenceClient implements ModelInferenceClient {

    private final AccountRecentSequenceReader sequenceReader;
    private final RuleBasedFallbackScorer fallbackScorer;
    private final TorchServeHttpCaller httpCaller;
    private final CircuitBreaker circuitBreaker;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TorchServeModelInferenceClient(
            AccountRecentSequenceReader sequenceReader,
            RuleBasedFallbackScorer fallbackScorer,
            TorchServeHttpCaller httpCaller,
            CircuitBreaker torchServeCircuitBreaker,
            MeterRegistry meterRegistry) {
        this.sequenceReader = sequenceReader;
        this.fallbackScorer = fallbackScorer;
        this.httpCaller = httpCaller;
        this.circuitBreaker = torchServeCircuitBreaker;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public FraudScore predict(String accountId) {
        List<RawFeatureStep> steps = sequenceReader.readRecentSteps(accountId);
        RawFeatureStep latestStep = steps.isEmpty() ? null : steps.get(steps.size() - 1);

        try {
            double probability = circuitBreaker.executeSupplier(() -> callTorchServe(accountId, steps));
            meterRegistry.counter("fds.fraud.score.count", "source", FraudScore.SOURCE_MODEL).increment();
            return new FraudScore(accountId, probability, FraudScore.SOURCE_MODEL);
        } catch (Exception e) {
            // CallNotPermittedException(서킷 오픈)부터 타임아웃/연결 실패/응답 파싱 실패까지 전부
            // 여기로 모인다 — 위 클래스 javadoc "실패로 간주하는 범위" 참고.
            log.warn("TorchServe 호출 실패, 규칙 기반 폴백으로 전환: accountId={}, cause={}",
                    accountId, e.toString());
            double fallbackProbability = Timer.builder("fds.fallback.scorer.latency")
                    .description("docs/PERFORMANCE_MEASUREMENT.md CP4 - 폴백 발생 시 규칙 기반 스코어 응답 latency")
                    .publishPercentileHistogram()
                    .register(meterRegistry)
                    .record(() -> fallbackScorer.score(latestStep));
            meterRegistry.counter("fds.fraud.score.count", "source", FraudScore.SOURCE_FALLBACK).increment();
            return new FraudScore(accountId, fallbackProbability, FraudScore.SOURCE_FALLBACK);
        }
    }

    private double callTorchServe(String accountId, List<RawFeatureStep> steps) {
        List<TorchServeTransactionStep> transactions = steps.stream()
                .map(TorchServeTransactionStep::from)
                .toList();
        TorchServePredictionRequest request = new TorchServePredictionRequest(accountId, transactions);

        String requestJson;
        try {
            requestJson = objectMapper.writeValueAsString(request);
        } catch (Exception e) {
            throw new IllegalStateException("TorchServe 요청 직렬화 실패: accountId=" + accountId, e);
        }

        String responseJson = httpCaller.call(requestJson);

        try {
            TorchServePredictionResponse response =
                    objectMapper.readValue(responseJson, TorchServePredictionResponse.class);
            return response.fraudProbability();
        } catch (Exception e) {
            throw new IllegalStateException("TorchServe 응답 파싱 실패: responseJson=" + responseJson, e);
        }
    }
}
