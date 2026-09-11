package com.fdsv2.modelclient;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * TorchServe REST 엔드포인트(serving/handler.py)를 실제로 호출하는 구현체.
 *
 * BACKEND.md 핵심 설계 결정("TorchServe는 REST 먼저, gRPC는 다음 단계")의 "REST 먼저" 부분 —
 * "다음 단계"는 {@link GrpcTorchServeHttpCaller}(backend/model-client-grpc-benchmark)로 구현됐다.
 * 기본값(matchIfMissing=true)이라 {@code fds.model-serving.torchserve.protocol}을 지정하지
 * 않으면 지금까지와 완전히 동일하게 동작한다.
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
@ConditionalOnProperty(
        prefix = "fds.model-serving.torchserve",
        name = "protocol",
        havingValue = "rest",
        matchIfMissing = true)
public class RestClientTorchServeHttpCaller implements TorchServeHttpCaller {

    private final RestClient restClient;
    private final String modelName;
    private final MeterRegistry meterRegistry;

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
        this.meterRegistry = meterRegistry;
    }

    @Override
    public String call(String requestBodyJson) {
        return Timer.builder("fds.model-serving.torchserve.caller.latency")
                .description("docs/PERFORMANCE_MEASUREMENT.md CP4 확장 - REST vs gRPC 호출 자체(직렬화+네트워크) latency")
                .tag("protocol", "rest")
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(() -> restClient.post()
                        .uri("/predictions/{modelName}", modelName)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(requestBodyJson)
                        .retrieve()
                        .body(String.class));
    }
}
