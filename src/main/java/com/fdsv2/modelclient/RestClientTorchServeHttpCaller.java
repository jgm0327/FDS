package com.fdsv2.modelclient;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * TorchServe REST 엔드포인트(serving/handler.py)를 실제로 호출하는 구현체.
 *
 * BACKEND.md 핵심 설계 결정("TorchServe는 REST 먼저, gRPC는 다음 단계")을 그대로 따른다.
 *
 * connect/read timeout을 모두 fds.model-serving.torchserve.timeout-ms로 통일한 이유: 이 값이
 * Resilience4j Circuit Breaker의 "얼마나 기다리다 실패로 칠지" 기준과 같아야, 서킷브레이커가
 * 열리는 시점을 이 한 값만으로 예측/조정할 수 있다 (connect/read를 따로 두면 실제 최대
 * 대기시간이 timeout-ms의 몇 배가 될 수 있어 CP4 성능 측정표의 "Circuit Breaker 타임아웃 값
 * 산정 근거"가 흐려진다).
 */
@Component
public class RestClientTorchServeHttpCaller implements TorchServeHttpCaller {

    private final RestClient restClient;
    private final String modelName;

    public RestClientTorchServeHttpCaller(
            @Value("${fds.model-serving.torchserve.base-url}") String baseUrl,
            @Value("${fds.model-serving.torchserve.timeout-ms}") long timeoutMs,
            @Value("${fds.model-serving.torchserve.model-name}") String modelName) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(timeoutMs));
        requestFactory.setReadTimeout(Duration.ofMillis(timeoutMs));

        this.restClient = RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(requestFactory)
                .build();
        this.modelName = modelName;
    }

    @Override
    public String call(String requestBodyJson) {
        return restClient.post()
                .uri("/predictions/{modelName}", modelName)
                .contentType(MediaType.APPLICATION_JSON)
                .body(requestBodyJson)
                .retrieve()
                .body(String.class);
    }
}
