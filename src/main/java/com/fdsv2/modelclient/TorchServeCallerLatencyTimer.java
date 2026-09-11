package com.fdsv2.modelclient;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * (backend/model-client-grpc-benchmark) {@code fds.model-serving.torchserve.caller.latency}
 * Timer를 만드는 공용 헬퍼 — {@link RestClientTorchServeHttpCaller}와
 * {@link GrpcTorchServeHttpCaller}가 같은 지표명/description/histogram 설정으로 protocol
 * 태그만 다르게 등록해야 REST/gRPC 비교가 성립한다. 두 클래스에 각자 복붙해두면 나중에 지표
 * 설정을 바꿀 때 한쪽만 고치고 다른 쪽을 놓치는 드리프트가 생기기 쉬워서 한 곳으로 모았다.
 *
 * <p>각 caller의 생성자에서 딱 한 번만 호출해서 필드로 캐싱해 둔다 — {@code call()}마다 매번
 * {@code Timer.builder(...).register(...)}를 새로 하면(코드 리뷰 지적) 요청마다 불필요하게
 * Meter.Id 생성 + 레지스트리 조회 비용이 붙는다. 캐싱해도 같은 이름/태그로 다시 등록하면
 * Micrometer가 기존 Timer를 그대로 반환하므로 의미상 차이는 없다.
 */
final class TorchServeCallerLatencyTimer {

    private TorchServeCallerLatencyTimer() {}

    static Timer create(MeterRegistry meterRegistry, String protocol) {
        return Timer.builder("fds.model-serving.torchserve.caller.latency")
                .description("docs/PERFORMANCE_MEASUREMENT.md CP4 확장 - REST vs gRPC 호출 자체(직렬화+네트워크) latency")
                .tag("protocol", protocol)
                .publishPercentileHistogram()
                .register(meterRegistry);
    }
}
