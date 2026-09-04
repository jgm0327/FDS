package com.fdsv2.modelclient;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * TorchServe 호출용 Resilience4j CircuitBreaker 배선.
 *
 * resilience4j-spring-boot3 스타터(auto-configuration)를 쓰지 않고 직접 빈으로 구성한 이유는
 * build.gradle의 의존성 추가 지점 주석 참고 — Spring Boot 4.1이 최신이라 스타터의
 * auto-configuration이 이 버전과 어긋날 위험을 피하기 위함이다.
 *
 * (backend/model-client-observability) 개별 {@code CircuitBreaker}를 바로 만들지 않고
 * {@link CircuitBreakerRegistry}를 거치는 이유: {@link TaggedCircuitBreakerMetrics}가
 * 레지스트리 단위로 등록된 모든 CircuitBreaker를 자동으로 찾아 지표를 바인딩하기 때문이다 —
 * CircuitBreaker가 하나뿐인 지금은 차이가 없지만, 나중에 다른 외부 호출(예: 다른 모델 버전,
 * 다른 서빙 엔드포인트)에 CircuitBreaker를 추가해도 이 배선을 그대로 재사용할 수 있다.
 *
 * <p><b>permittedNumberOfCallsInHalfOpenState를 명시적으로 낮게(기본 3) 잡은 이유</b> — CP4 k6
 * 장애 주입 테스트(k6/cp4-model-client-fault-injection-test.js) 중 실제로 겪은 문제: resilience4j
 * 기본값(10)을 그대로 쓰면, TorchServe가 다시 살아나도 서킷브레이커가 CLOSED로 복구를 못 하고
 * OPEN에 계속 머무는 현상을 재현했다. 원인은 "동시성 10건짜리 부하 자체가 매번 재현되는
 * thundering herd"였다 — k6가 VU 10개로 계속 동시 요청을 보내는 상황에서 HALF_OPEN으로
 * 전환되는 순간, 그 10개 VU의 요청이 거의 동시에 "시험 호출"로 몰려 들어간다. TorchServe는
 * 기본 워커가 1개뿐이라(ai/README.md TorchServe 배포 절 참고) 이 몰린 요청들이 순차 처리되며
 * 대기시간이 누적되고, 그중 일부가 timeout-ms(300ms)를 넘겨 다시 실패로 잡힌다 — 결과적으로
 * "복구를 시험하는 행위 자체"가 다시 장애를 재현하는 악순환이 되어 서킷이 영원히 안 닫혔다.
 * (순차 호출로 바꿔서 재현했을 땐 정상적으로 HALF_OPEN -> CLOSED로 복구되는 것을 확인함 —
 * 세션 로그에 실측 타임라인 기록.) permittedNumberOfCallsInHalfOpenState를 3으로 낮추면 회복
 * 시도 자체의 동시 요청 수가 줄어 TorchServe 단일 워커가 감당할 수 있는 범위 안에 들어온다.
 * 근본적인 해결책(TorchServe 워커 수 증설)은 AI 쪽 배포 설정 변경이 필요해 이번 범위 밖으로
 * 남겨둔다.
 */
@Configuration
public class ModelClientConfig {

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry(
            @Value("${fds.model-serving.torchserve.circuit-breaker.failure-rate-threshold}") float failureRateThreshold,
            @Value("${fds.model-serving.torchserve.circuit-breaker.wait-duration-in-open-state-seconds}") long waitDurationSeconds,
            @Value("${fds.model-serving.torchserve.circuit-breaker.sliding-window-size}") int slidingWindowSize,
            @Value("${fds.model-serving.torchserve.circuit-breaker.minimum-number-of-calls}") int minimumNumberOfCalls,
            @Value("${fds.model-serving.torchserve.circuit-breaker.permitted-calls-in-half-open-state}") int permittedCallsInHalfOpenState) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(Duration.ofSeconds(waitDurationSeconds))
                .slidingWindowSize(slidingWindowSize)
                .minimumNumberOfCalls(minimumNumberOfCalls)
                .permittedNumberOfCallsInHalfOpenState(permittedCallsInHalfOpenState)
                .build();
        return CircuitBreakerRegistry.of(config);
    }

    @Bean
    public CircuitBreaker torchServeCircuitBreaker(CircuitBreakerRegistry circuitBreakerRegistry) {
        return circuitBreakerRegistry.circuitBreaker("torchserve");
    }

    /**
     * docs/PERFORMANCE_MEASUREMENT.md CP4 - "타임아웃/서킷브레이커 오픈 발생률 → Resilience4j
     * metrics → Prometheus"에 대응. 노출되는 주요 지표:
     * {@code resilience4j_circuitbreaker_state}(현재 상태), {@code resilience4j_circuitbreaker_calls_seconds}
     * (호출 결과별 latency, kind=successful/failed/...), {@code resilience4j_circuitbreaker_failure_rate}.
     */
    @Bean
    public TaggedCircuitBreakerMetrics taggedCircuitBreakerMetrics(
            CircuitBreakerRegistry circuitBreakerRegistry, MeterRegistry meterRegistry) {
        TaggedCircuitBreakerMetrics metrics = TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry);
        metrics.bindTo(meterRegistry);
        return metrics;
    }
}
