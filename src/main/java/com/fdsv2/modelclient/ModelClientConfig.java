package com.fdsv2.modelclient;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedBulkheadMetrics;
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
 *
 * <p><b>(backend/model-client-concurrency-fix) 위 "미해결" 문제 재검증 결과 — 가설 1(TorchServe
 * 단일 워커가 동시 부하를 못 버틴다) 확정, 가설 2(로컬 환경 오염) 기각</b> — 앱/TorchServe/Docker를
 * 전부 새로 띄운 완전히 깨끗한 상태에서 k6 10 VU 부하를 재시작 없이 딱 한 번 실행한 결과,
 * TorchServe를 전혀 건드리지 않았는데도 45초 동안 3709건 중 964건(26%)이 FALLBACK으로
 * 전환되는 걸 재현했다 — "환경이 지저분해서" 생기는 문제가 아니라 동시성 자체가 원인임을
 * 확인. TorchServe 관리 API(`GET /models/{name}`)로 확인한 실제 배포 설정은
 * {@code minWorkers=1, maxWorkers=1} — k6 10 VU가 동시에 쏘는 요청을 워커 1개가 순차 처리하며
 * 큐잉되고, 그 대기시간이 timeout-ms(300ms)를 넘겨 실패로 잡히는 것이 실제 메커니즘이었다.
 *
 * <p>{@link Bulkhead}를 추가로 얹은 이유: 워커 수 증설(ai/pytorch-sequence-model 배포 설정,
 * ai/README.md 참고)이 근본 대책이지만, 그것만으로는 "배포된 워커 수보다 클라이언트 동시
 * 요청이 많아지는 상황"이 재발할 수 있다 — 예를 들어 트래픽이 늘거나 워커 수 설정이 나중에
 * 바뀌면 같은 문제가 다시 재현된다. Bulkhead로 앱이 TorchServe에 동시에 흘려보내는 호출 수
 * 자체를 배포된 워커 수만큼으로 제한해두면, 초과분은 TorchServe까지 가지도 않고 즉시
 * BulkheadFullException으로 걸러져 곧바로 폴백으로 빠진다 — 큐잉 대기 후 타임아웃으로 잡히는
 * 것보다 훨씬 빠르고, 무엇보다 Circuit Breaker가 "TorchServe가 실제로 응답한 실패"만 보게 되어
 * 상태 지표의 신뢰도가 올라간다(TorchServeModelInferenceClient에서 Bulkhead를 Circuit Breaker
 * 바깥쪽에 감싸는 순서 참고 — Bulkhead가 거른 호출은 Circuit Breaker 통계에 아예 안 잡힌다).
 * {@code maxConcurrentCalls}는 실제 배포된 TorchServe 워커 수와 함께 튜닝해야 하는 값이라
 * 코드에 고정하지 않고 설정으로 뺐다.
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

    /**
     * TorchServe에 동시에 흘려보내는 호출 수 상한 — 클래스 javadoc의 "Bulkhead를 추가로 얹은
     * 이유" 참고. {@code maxWaitDuration=0}(대기 없이 즉시 거절)인 이유: 대기시켰다가 순서가
     * 와도 어차피 timeout-ms 안에 못 끝날 수 있다는 게 이번에 겪은 문제의 본질이라, 대기시키는
     * 대신 초과분은 곧바로 폴백으로 보내는 편이 낫다고 판단.
     */
    @Bean
    public BulkheadRegistry bulkheadRegistry(
            @Value("${fds.model-serving.torchserve.bulkhead.max-concurrent-calls}") int maxConcurrentCalls) {
        BulkheadConfig config = BulkheadConfig.custom()
                .maxConcurrentCalls(maxConcurrentCalls)
                .maxWaitDuration(Duration.ZERO)
                .build();
        return BulkheadRegistry.of(config);
    }

    @Bean
    public Bulkhead torchServeBulkhead(BulkheadRegistry bulkheadRegistry) {
        return bulkheadRegistry.bulkhead("torchserve");
    }

    /**
     * {@code resilience4j_bulkhead_available_concurrent_calls}(남은 여유), {@code
     * resilience4j_bulkhead_max_allowed_concurrent_calls} 노출 — CP4 성능 측정표의 "동시
     * 요청이 TorchServe 워커 capacity를 넘는지"를 Grafana에서 직접 확인하기 위함
     * (TaggedCircuitBreakerMetrics와 같은 패턴, CircuitBreaker 지표만으로는 "왜" 실패가
     * 늘었는지(용량 초과 vs 실제 장애)를 구분할 수 없어서 추가).
     */
    @Bean
    public TaggedBulkheadMetrics taggedBulkheadMetrics(BulkheadRegistry bulkheadRegistry, MeterRegistry meterRegistry) {
        TaggedBulkheadMetrics metrics = TaggedBulkheadMetrics.ofBulkheadRegistry(bulkheadRegistry);
        metrics.bindTo(meterRegistry);
        return metrics;
    }
}
