package com.fdsv2.modelclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
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
 */
@Slf4j
@Component
public class TorchServeModelInferenceClient implements ModelInferenceClient {

    private final AccountRecentSequenceReader sequenceReader;
    private final RuleBasedFallbackScorer fallbackScorer;
    private final TorchServeHttpCaller httpCaller;
    private final CircuitBreaker circuitBreaker;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TorchServeModelInferenceClient(
            AccountRecentSequenceReader sequenceReader,
            RuleBasedFallbackScorer fallbackScorer,
            TorchServeHttpCaller httpCaller,
            CircuitBreaker torchServeCircuitBreaker) {
        this.sequenceReader = sequenceReader;
        this.fallbackScorer = fallbackScorer;
        this.httpCaller = httpCaller;
        this.circuitBreaker = torchServeCircuitBreaker;
    }

    @Override
    public FraudScore predict(String accountId) {
        List<RawFeatureStep> steps = sequenceReader.readRecentSteps(accountId);
        RawFeatureStep latestStep = steps.isEmpty() ? null : steps.get(steps.size() - 1);

        try {
            double probability = circuitBreaker.executeSupplier(() -> callTorchServe(accountId, steps));
            return new FraudScore(accountId, probability, FraudScore.SOURCE_MODEL);
        } catch (Exception e) {
            // CallNotPermittedException(서킷 오픈)부터 타임아웃/연결 실패/응답 파싱 실패까지 전부
            // 여기로 모인다 — 위 클래스 javadoc "실패로 간주하는 범위" 참고.
            log.warn("TorchServe 호출 실패, 규칙 기반 폴백으로 전환: accountId={}, cause={}",
                    accountId, e.toString());
            return new FraudScore(accountId, fallbackScorer.score(latestStep), FraudScore.SOURCE_FALLBACK);
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
