package com.fdsv2.modelclient;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.pytorch.serve.grpc.inference.InferenceAPIsServiceGrpc;
import org.pytorch.serve.grpc.inference.PredictionResponse;
import org.pytorch.serve.grpc.inference.PredictionsRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * TorchServe gRPC 추론 API({@code src/main/proto/inference.proto}, TorchServe 공식 정의를
 * 그대로 가져온 것)를 호출하는 구현체 — BACKEND.md 핵심 설계 결정("TorchServe는 REST 먼저,
 * gRPC는 다음 단계")의 "다음 단계" (backend/model-client-grpc-benchmark).
 *
 * <p>{@link TorchServeHttpCaller}와 똑같은 좁은 인터페이스를 구현하므로,
 * {@link TorchServeModelInferenceClient}는 REST/gRPC 어느 쪽이 활성화됐는지 전혀 몰라도 된다 —
 * Circuit Breaker/Bulkhead/폴백 로직을 하나도 안 건드리고 전송 계층만 교체한 것.
 * {@code fds.model-serving.torchserve.protocol=grpc}로 이 빈이 활성화된다(기본은 REST).
 *
 * <p><b>요청 바디를 어떻게 gRPC로 옮기는가</b>: REST는 JSON을 그대로 HTTP 바디로 보내지만, gRPC
 * {@code PredictionsRequest}는 {@code map<string, bytes> input}이다. TorchServe 프론트엔드가
 * gRPC 요청을 커스텀 핸들러(`serving/handler.py`)로 넘길 때 이 map의 각 엔트리를 그대로 그
 * 핸들러의 {@code data} 리스트 항목(dict)으로 펼쳐 넣는다 — `preprocess()`가
 * {@code row.get("body") or row.get("data")}로 REST/gRPC 양쪽을 다 받아주는 이유가 이거다
 * (REST는 TorchServe가 자동으로 "body" 키에 원본 바이트를 넣어주고, gRPC는 우리가 지정한 map
 * 키가 그대로 쓰인다). 그래서 키를 "data"로 통일해서 넣으면 핸들러 코드를 전혀 안 고쳐도 된다.
 *
 * <p><b>채널을 매 호출마다 새로 안 만드는 이유</b>: gRPC의 {@link ManagedChannel}은 HTTP/2 커넥션을
 * 재사용하도록 설계된 무거운 객체라, REST의 커넥션 풀과 같은 역할을 한다 — 빈 생성 시 한 번만
 * 만들어서 애플리케이션 생명주기 동안 재사용하고, {@link PreDestroy}에서 정리한다.
 *
 * <p><b>usePlaintext()를 쓰는 이유</b>: 이 프로젝트의 TorchServe는 로컬 검증 목적이라 TLS를
 * 설정하지 않았다(ai/README.md TorchServe 배포 절 참고) — REST 쪽도 http(평문)를 쓰는 것과
 * 같은 이유로 대칭을 맞췄다. 실제 운영 배포라면 이 부분에 TLS를 반드시 추가해야 한다.
 */
@Component
@ConditionalOnProperty(prefix = "fds.model-serving.torchserve", name = "protocol", havingValue = "grpc")
public class GrpcTorchServeHttpCaller implements TorchServeHttpCaller {

    private static final String INPUT_KEY = "data";

    private final ManagedChannel channel;
    private final InferenceAPIsServiceGrpc.InferenceAPIsServiceBlockingStub stub;
    private final String modelName;
    private final long timeoutMs;
    private final MeterRegistry meterRegistry;

    public GrpcTorchServeHttpCaller(
            @Value("${fds.model-serving.torchserve.grpc-host:localhost}") String grpcHost,
            @Value("${fds.model-serving.torchserve.grpc-port:7070}") int grpcPort,
            @Value("${fds.model-serving.torchserve.timeout-ms}") long timeoutMs,
            @Value("${fds.model-serving.torchserve.model-name}") String modelName,
            MeterRegistry meterRegistry) {
        this.channel = NettyChannelBuilder.forAddress(grpcHost, grpcPort)
                .usePlaintext()
                .build();
        this.stub = InferenceAPIsServiceGrpc.newBlockingStub(channel);
        this.modelName = modelName;
        this.timeoutMs = timeoutMs;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public String call(String requestBodyJson) {
        return Timer.builder("fds.model-serving.torchserve.caller.latency")
                .description("docs/PERFORMANCE_MEASUREMENT.md CP4 확장 - REST vs gRPC 호출 자체(직렬화+네트워크) latency")
                .tag("protocol", "grpc")
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(() -> {
                    PredictionsRequest request = PredictionsRequest.newBuilder()
                            .setModelName(modelName)
                            .putInput(INPUT_KEY, ByteString.copyFrom(requestBodyJson, StandardCharsets.UTF_8))
                            .build();
                    PredictionResponse response = stub
                            .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                            .predictions(request);
                    return response.getPrediction().toStringUtf8();
                });
    }

    @PreDestroy
    void shutdown() {
        channel.shutdownNow();
    }
}
