package com.fdsv2.modelclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.grpc.StatusRuntimeException;
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
 *
 * <p>(backend/model-client-concurrency-fix) {@link Bulkhead}를 {@link CircuitBreaker} 바깥쪽에
 * 감싸는 순서 — {@code bulkhead.executeSupplier(() -> circuitBreaker.executeSupplier(...))}.
 * Bulkhead가 먼저 동시 호출 수를 걸러내므로, 이미 가득 찬 상태에서 거절된 호출(BulkheadFullException)은
 * CircuitBreaker.executeSupplier 자체가 호출되지 않아 CircuitBreaker의 성공/실패 통계에 전혀
 * 잡히지 않는다 — ModelClientConfig 클래스 javadoc "Bulkhead를 추가로 얹은 이유" 참고. 반대
 * 순서(CircuitBreaker가 바깥)로 하면 Bulkhead 거절도 CircuitBreaker의 "실패"로 집계되어, 정작
 * TorchServe가 멀쩡한데도 자기 자신의 동시성 제한 때문에 서킷이 열리는 또 다른 자기 참조적
 * 문제가 생긴다.
 */
@Slf4j
@Component
public class TorchServeModelInferenceClient implements ModelInferenceClient {

    private final AccountRecentSequenceReader sequenceReader;
    private final RuleBasedFallbackScorer fallbackScorer;
    private final TorchServeHttpCaller httpCaller;
    private final CircuitBreaker circuitBreaker;
    private final Bulkhead bulkhead;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TorchServeModelInferenceClient(
            AccountRecentSequenceReader sequenceReader,
            RuleBasedFallbackScorer fallbackScorer,
            TorchServeHttpCaller httpCaller,
            CircuitBreaker torchServeCircuitBreaker,
            Bulkhead torchServeBulkhead,
            MeterRegistry meterRegistry) {
        this.sequenceReader = sequenceReader;
        this.fallbackScorer = fallbackScorer;
        this.httpCaller = httpCaller;
        this.circuitBreaker = torchServeCircuitBreaker;
        this.bulkhead = torchServeBulkhead;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public FraudScore predict(String accountId) {
        // 코드 리뷰 지적: 이 조회가 try 밖에 있으면 Redis 장애 시(동시 부하로 커넥션 풀이
        // 고갈되는 경우 등, 이 클래스가 Bulkhead로 막으려는 것과 같은 종류의 부하 상황에서 실제로
        // 벌어질 수 있음) 이 클래스의 핵심 계약("predict()는 항상 FraudScore를 반환하며 예외를
        // 던지지 않는다")이 깨진다. try 안으로 옮기고, 실패하면 latestStep 없이(null) 곧바로
        // 규칙 기반 폴백으로 빠진다.
        List<RawFeatureStep> steps;
        RawFeatureStep latestStep;
        try {
            steps = sequenceReader.readRecentSteps(accountId);
            latestStep = steps.isEmpty() ? null : steps.get(steps.size() - 1);
        } catch (Exception e) {
            log.warn("최근 거래 시퀀스 조회 실패, 규칙 기반 폴백으로 전환: accountId={}, cause={}",
                    accountId, e.toString());
            return fallback(accountId, null, "sequence_read_error");
        }

        try {
            double probability = bulkhead.executeSupplier(
                    () -> circuitBreaker.executeSupplier(() -> callTorchServe(accountId, steps)));
            meterRegistry.counter("fds.fraud.score.count", "source", FraudScore.SOURCE_MODEL).increment();
            return new FraudScore(accountId, probability, FraudScore.SOURCE_MODEL);
        } catch (Exception e) {
            // 코드 리뷰 지적: 이 catch가 BulkheadFullException(동시 호출 한도 초과)/
            // CallNotPermittedException(서킷 오픈)부터 타임아웃/연결 실패/응답 파싱 실패까지 전부
            // 뭉뚱그려서(위 클래스 javadoc "실패로 간주하는 범위"), 정작 Bulkhead/CircuitBreaker를
            // 나눠서 얹은 목적("용량 초과로 인한 거절"과 "TorchServe의 실제 실패"를 구분)이 운영자가
            // 보는 로그/폴백 사유 태그에서는 다시 합쳐진다. 예외 타입으로 사유를 구분해서 태그를
            // 남긴다 — 동시성 원인 조사가 다음에 또 필요할 때 CircuitBreaker 통계까지 안 뒤져도
            // 이 태그만으로 원인을 좁힐 수 있게.
            String reason = fallbackReason(e);
            log.warn("TorchServe 호출 실패, 규칙 기반 폴백으로 전환: accountId={}, reason={}, cause={}",
                    accountId, reason, e.toString());
            return fallback(accountId, latestStep, reason);
        }
    }

    private static String fallbackReason(Exception e) {
        if (e instanceof BulkheadFullException) {
            return "bulkhead_full";
        }
        if (e instanceof CallNotPermittedException) {
            return "circuit_open";
        }
        // (backend/model-client-grpc-benchmark) 코드 리뷰 지적: protocol=grpc일 때는 모든 실패가
        // StatusRuntimeException으로 온다 — 이걸 REST 실패와 똑같이 "torchserve_error"로 뭉개면
        // DEADLINE_EXCEEDED(타임아웃)와 UNAVAILABLE(연결 불가) 같은, 원인이 서로 다른 gRPC
        // 실패를 운영자가 구분할 방법이 없어진다. gRPC의 상태 코드(getStatus().getCode())를 그대로
        // 태그에 반영해서 REST의 "torchserve_error" 하나로 뭉뚱그리던 것과 같은 수준의 세분화를
        // gRPC 쪽에도 맞춘다.
        if (e instanceof StatusRuntimeException grpcException) {
            return "grpc_" + grpcException.getStatus().getCode().name().toLowerCase();
        }
        return "torchserve_error";
    }

    private FraudScore fallback(String accountId, RawFeatureStep latestStep, String reason) {
        double fallbackProbability = Timer.builder("fds.fallback.scorer.latency")
                .description("docs/PERFORMANCE_MEASUREMENT.md CP4 - 폴백 발생 시 규칙 기반 스코어 응답 latency")
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(() -> fallbackScorer.score(latestStep));
        meterRegistry.counter("fds.fraud.score.count", "source", FraudScore.SOURCE_FALLBACK, "reason", reason)
                .increment();
        return new FraudScore(accountId, fallbackProbability, FraudScore.SOURCE_FALLBACK);
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
