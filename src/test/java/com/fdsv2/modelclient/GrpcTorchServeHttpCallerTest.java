package com.fdsv2.modelclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import com.google.rpc.Status;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pytorch.serve.grpc.inference.InferenceAPIsServiceGrpc;
import org.pytorch.serve.grpc.inference.PredictionResponse;
import org.pytorch.serve.grpc.inference.PredictionsRequest;

/**
 * 진짜 TorchServe 없이 로컬 임시 포트에 가짜 {@code InferenceAPIsService} 서버를 띄워서
 * {@link GrpcTorchServeHttpCaller}가 프로토콜 변환(요청을 "data" 키에 담기, 응답 바이트를
 * 문자열로 디코딩)을 정확히 하는지만 검증한다 — 실제 TorchServe와의 REST/gRPC latency 비교는
 * docs/sessions 세션 로그의 k6 실측 결과 참고.
 */
class GrpcTorchServeHttpCallerTest {

    private Server server;
    private GrpcTorchServeHttpCaller caller;
    private final AtomicReference<PredictionsRequest> lastRequest = new AtomicReference<>();

    @BeforeEach
    void setUp() throws IOException {
        server = ServerBuilder.forPort(0)
                .addService(new InferenceAPIsServiceGrpc.InferenceAPIsServiceImplBase() {
                    @Override
                    public void predictions(
                            PredictionsRequest request, StreamObserver<PredictionResponse> responseObserver) {
                        lastRequest.set(request);
                        responseObserver.onNext(PredictionResponse.newBuilder()
                                .setPrediction(ByteString.copyFromUtf8(
                                        "{\"accountId\":\"acc-1\",\"fraudProbability\":0.42}"))
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        caller = new GrpcTorchServeHttpCaller(
                "localhost", server.getPort(), 1000, "fds-sequence-model", new SimpleMeterRegistry());
    }

    @AfterEach
    void tearDown() {
        caller.shutdown();
        server.shutdownNow();
    }

    @Test
    void 요청_바디를_data_키에_담아_모델명과_함께_보낸다() {
        caller.call("{\"accountId\":\"acc-1\"}");

        PredictionsRequest sent = lastRequest.get();
        assertThat(sent.getModelName()).isEqualTo("fds-sequence-model");
        assertThat(sent.getInputMap().get("data").toStringUtf8()).isEqualTo("{\"accountId\":\"acc-1\"}");
    }

    @Test
    void 응답의_prediction_바이트를_문자열로_그대로_반환한다() {
        String result = caller.call("{\"accountId\":\"acc-1\"}");

        assertThat(result).isEqualTo("{\"accountId\":\"acc-1\",\"fraudProbability\":0.42}");
    }

    @Test
    void gRPC_레벨_오류_응답은_예외로_전파된다() throws IOException {
        // TorchServeHttpCaller 인터페이스 계약(@throws RuntimeException) 회귀 테스트 — 성공
        // 경로만 검증하던 기존 테스트로는 이 계약이 깨져도(예: 예외를 삼키고 빈 문자열을 반환)
        // 잡히지 않았다.
        Server errorServer = ServerBuilder.forPort(0)
                .addService(new InferenceAPIsServiceGrpc.InferenceAPIsServiceImplBase() {
                    @Override
                    public void predictions(
                            PredictionsRequest request, StreamObserver<PredictionResponse> responseObserver) {
                        responseObserver.onError(io.grpc.Status.UNAVAILABLE.withDescription("model down").asRuntimeException());
                    }
                })
                .build()
                .start();
        GrpcTorchServeHttpCaller errorCaller = new GrpcTorchServeHttpCaller(
                "localhost", errorServer.getPort(), 1000, "fds-sequence-model", new SimpleMeterRegistry());

        try {
            assertThatThrownBy(() -> errorCaller.call("{\"accountId\":\"acc-1\"}"))
                    .isInstanceOf(StatusRuntimeException.class);
        } finally {
            errorCaller.shutdown();
            errorServer.shutdownNow();
        }
    }

    @Test
    void 응답_메시지_안에_오류_상태가_담겨있으면_예외를_던진다() {
        // inference.proto의 status 필드는 주로 스트리밍용이라 단항 Predictions RPC에서 채워질
        // 일은 거의 없지만, GrpcTorchServeHttpCaller가 이 필드를 무시하지 않고 실제로 확인하는지
        // 검증한다 — 무시하면 오류가 "성공"으로 취급돼 빈 prediction 바이트가 그대로 반환된다.
        // setUp()의 공용 server/caller와는 별개 포트를 쓰므로 건드리지 않는다.
        Server statusErrorServer = null;
        try {
            statusErrorServer = ServerBuilder.forPort(0)
                    .addService(new InferenceAPIsServiceGrpc.InferenceAPIsServiceImplBase() {
                        @Override
                        public void predictions(
                                PredictionsRequest request, StreamObserver<PredictionResponse> responseObserver) {
                            responseObserver.onNext(PredictionResponse.newBuilder()
                                    .setStatus(Status.newBuilder()
                                            .setCode(com.google.rpc.Code.INTERNAL_VALUE)
                                            .setMessage("internal error")
                                            .build())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    })
                    .build()
                    .start();
            GrpcTorchServeHttpCaller statusErrorCaller = new GrpcTorchServeHttpCaller(
                    "localhost", statusErrorServer.getPort(), 1000, "fds-sequence-model", new SimpleMeterRegistry());

            assertThatThrownBy(() -> statusErrorCaller.call("{\"accountId\":\"acc-1\"}"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("internal error");

            statusErrorCaller.shutdown();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (statusErrorServer != null) {
                statusErrorServer.shutdownNow();
            }
        }
    }
}
