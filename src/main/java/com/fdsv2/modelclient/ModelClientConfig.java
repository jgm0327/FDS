package com.fdsv2.modelclient;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
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
 */
@Configuration
public class ModelClientConfig {

    @Bean
    public CircuitBreaker torchServeCircuitBreaker(
            @Value("${fds.model-serving.torchserve.circuit-breaker.failure-rate-threshold}") float failureRateThreshold,
            @Value("${fds.model-serving.torchserve.circuit-breaker.wait-duration-in-open-state-seconds}") long waitDurationSeconds,
            @Value("${fds.model-serving.torchserve.circuit-breaker.sliding-window-size}") int slidingWindowSize,
            @Value("${fds.model-serving.torchserve.circuit-breaker.minimum-number-of-calls}") int minimumNumberOfCalls) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(Duration.ofSeconds(waitDurationSeconds))
                .slidingWindowSize(slidingWindowSize)
                .minimumNumberOfCalls(minimumNumberOfCalls)
                .build();
        return CircuitBreaker.of("torchserve", config);
    }
}
