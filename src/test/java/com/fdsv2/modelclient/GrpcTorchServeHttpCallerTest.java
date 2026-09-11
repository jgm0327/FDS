package com.fdsv2.modelclient;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
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
}
