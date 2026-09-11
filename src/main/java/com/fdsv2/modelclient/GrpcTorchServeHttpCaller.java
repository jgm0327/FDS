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
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

/**
 * TorchServe gRPC 추론 API({@code src/main/proto/inference.proto}, TorchServe 공식 정의를
 * 그대로 가져온 것)를 호출하는 구현체 — BACKEND.md 핵심 설계 결정("TorchServe는 REST 먼저,
 * gRPC는 다음 단계")의 "다음 단계" (backend/model-client-grpc-benchmark).
 *
 * <p>{@link TorchServeHttpCaller}와 똑같은 좁은 인터페이스를 구현하므로,
 * {@link TorchServeModelInferenceClient}는 REST/gRPC 어느 쪽이 활성화됐는지 전혀 몰라도 된다 —
 * Circuit Breaker/Bulkhead/폴백 로직을 하나도 안 건드리고 전송 계층만 교체한 것.
 * {@code fds.model-serving.torchserve.protocol=grpc}일 때만 이 빈이 활성화된다(그 외 값은
 * 전부 {@link RestClientTorchServeHttpCaller} 참고 — 오타 시 기동 실패 대신 REST로 안전하게
 * 떨어지도록 조건을 대칭으로 맞췄다).
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
 * <p><b>keepAlive를 켠 이유(코드 리뷰 지적)</b>: {@code withDeadlineAfter(timeoutMs)}는 RPC 하나의
 * 전체 예산이라, 유휴 상태에서 커넥션이 끊겨 재연결까지 필요해지면 그 재연결 비용까지 이 좁은
 * 예산 안에서 처리해야 한다 — REST(RestClientTorchServeHttpCaller)는 connect/read timeout을
 * 각각 timeoutMs만큼 따로 주는 것과 비대칭이었다. keepAliveTime/keepAliveWithoutCalls로 유휴
 * 중에도 커넥션을 계속 살려둬서, 이 프로젝트의 Bulkhead(항상 어느 정도 트래픽 유지) 시나리오에서
 * 재연결이 RPC 데드라인을 잠식하는 상황 자체를 줄인다 — deadline을 connect/call로 분리하는 것
 * 자체는 gRPC가 기본 지원하지 않아 대신 택한 완화책.
 *
 * <p><b>usePlaintext()를 쓰는 이유</b>: 이 프로젝트의 TorchServe는 로컬 검증 목적이라 TLS를
 * 설정하지 않았다(ai/README.md TorchServe 배포 절 참고) — REST 쪽도 http(평문)를 쓰는 것과
 * 같은 이유로 대칭을 맞췄다. 실제 운영 배포라면 이 부분에 TLS를 반드시 추가해야 한다.
 */
@Component
@ConditionalOnExpression("'grpc'.equalsIgnoreCase('${fds.model-serving.torchserve.protocol:rest}')")
public class GrpcTorchServeHttpCaller implements TorchServeHttpCaller {

    private static final String INPUT_KEY = "data";
    private static final long KEEP_ALIVE_TIME_SECONDS = 30;
    private static final long KEEP_ALIVE_TIMEOUT_SECONDS = 5;

    private final ManagedChannel channel;
    private final InferenceAPIsServiceGrpc.InferenceAPIsServiceBlockingStub stub;
    private final String modelName;
    private final long timeoutMs;
    private final Timer callLatencyTimer;

    public GrpcTorchServeHttpCaller(
            @Value("${fds.model-serving.torchserve.grpc-host:localhost}") String grpcHost,
            @Value("${fds.model-serving.torchserve.grpc-port:7070}") int grpcPort,
            @Value("${fds.model-serving.torchserve.timeout-ms}") long timeoutMs,
            @Value("${fds.model-serving.torchserve.model-name}") String modelName,
            MeterRegistry meterRegistry) {
        this.channel = NettyChannelBuilder.forAddress(grpcHost, grpcPort)
                .usePlaintext()
                .keepAliveTime(KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS)
                .keepAliveTimeout(KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .build();
        this.stub = InferenceAPIsServiceGrpc.newBlockingStub(channel);
        this.modelName = modelName;
        this.timeoutMs = timeoutMs;
        this.callLatencyTimer = TorchServeCallerLatencyTimer.create(meterRegistry, "grpc");
    }

    @Override
    public String call(String requestBodyJson) {
        return callLatencyTimer.record(() -> {
            PredictionsRequest request = PredictionsRequest.newBuilder()
                    .setModelName(modelName)
                    .putInput(INPUT_KEY, ByteString.copyFrom(requestBodyJson, StandardCharsets.UTF_8))
                    .build();
            PredictionResponse response = stub
                    .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .predictions(request);
            // inference.proto의 status 필드는 주로 StreamPredictions2(양방향 스트리밍)용이라
            // 지금 쓰는 단항 Predictions RPC에서 채워질 일은 거의 없지만, 방어적으로 확인한다 —
            // 안 하면 오류가 보고돼도 조용히 "성공"으로 취급하고 빈/이상 바이트를 그대로
            // 반환해서, 나중에 JSON 파싱 실패로만 보이고 진짜 원인(gRPC 레벨 오류)이 가려진다.
            if (response.hasStatus() && response.getStatus().getCode() != 0) {
                throw new IllegalStateException(
                        "TorchServe gRPC 응답에 오류 상태 포함: " + response.getStatus());
            }
            return response.getPrediction().toStringUtf8();
        });
    }

    @PreDestroy
    void shutdown() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
