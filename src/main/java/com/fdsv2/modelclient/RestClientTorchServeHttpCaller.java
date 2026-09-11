package com.fdsv2.modelclient;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * TorchServe REST 엔드포인트(serving/handler.py)를 실제로 호출하는 구현체.
 *
 * BACKEND.md 핵심 설계 결정("TorchServe는 REST 먼저, gRPC는 다음 단계")의 "REST 먼저" 부분 —
 * "다음 단계"는 {@link GrpcTorchServeHttpCaller}(backend/model-client-grpc-benchmark)로 구현됐다.
 *
 * <p>조건을 "protocol == grpc가 아니면 전부"로 잡은 이유(코드 리뷰 지적): {@code @ConditionalOnProperty}로
 * havingValue="rest"를 정확히 매칭시키면, 오타/대소문자(예: "REST", "Grpc")가 들어왔을 때 이
 * 빈도 {@link GrpcTorchServeHttpCaller}도 활성화 안 돼서 {@link TorchServeHttpCaller} 빈이
 * 하나도 없는 채로 기동에 실패한다 — 원인을 짐작하기 어려운
 * {@code UnsatisfiedDependencyException}만 던지고 끝난다. "grpc를 명시적으로 지정한 경우만
 * gRPC, 그 외(오타 포함)는 전부 REST"로 잡으면 최소한 기동은 항상 되고, REST가 원래 기본값이라는
 * 취지도 그대로 유지된다.
 *
 * connect/read timeout을 모두 fds.model-serving.torchserve.timeout-ms로 통일한 이유: 이 값이
 * Resilience4j Circuit Breaker의 "얼마나 기다리다 실패로 칠지" 기준과 같아야, 서킷브레이커가
 * 열리는 시점을 이 한 값만으로 예측/조정할 수 있다 (connect/read를 따로 두면 실제 최대
 * 대기시간이 timeout-ms의 몇 배가 될 수 있어 CP4 성능 측정표의 "Circuit Breaker 타임아웃 값
 * 산정 근거"가 흐려진다).
 *
 * <p>(backend/model-client-grpc-benchmark) {@code fds.model-serving.torchserve.caller.latency}
 * (태그 protocol=rest)는 {@link GrpcTorchServeHttpCaller}와 동일 지표명/태그 체계로 REST/gRPC를
 * 나란히 비교하기 위한 것 — Redis 조회, Circuit Breaker/Bulkhead 오버헤드는 제외하고 "직렬화 +
 * 네트워크 왕복"만 감싼다({@link TorchServeModelInferenceClient}가 아니라 이 클래스에서 직접
 * 감싸는 이유).
 */
@Component
@ConditionalOnExpression("!'grpc'.equalsIgnoreCase('${fds.model-serving.torchserve.protocol:rest}')")
public class RestClientTorchServeHttpCaller implements TorchServeHttpCaller {

    private final RestClient restClient;
    private final String modelName;
    private final Timer callLatencyTimer;

    public RestClientTorchServeHttpCaller(
            @Value("${fds.model-serving.torchserve.base-url}") String baseUrl,
            @Value("${fds.model-serving.torchserve.timeout-ms}") long timeoutMs,
            @Value("${fds.model-serving.torchserve.model-name}") String modelName,
            MeterRegistry meterRegistry) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(timeoutMs));
        requestFactory.setReadTimeout(Duration.ofMillis(timeoutMs));

        this.restClient = RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
        this.modelName = modelName;
        this.callLatencyTimer = TorchServeCallerLatencyTimer.create(meterRegistry, "rest");
    }

    @Override
    public String call(String requestBodyJson) {
        return callLatencyTimer.record(() -> restClient.post()
                .uri("/predictions/{modelName}", modelName)
                .contentType(MediaType.APPLICATION_JSON)
                .body(requestBodyJson)
                .retrieve()
                .body(String.class));
    }
}
