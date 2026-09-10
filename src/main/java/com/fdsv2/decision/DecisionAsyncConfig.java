package com.fdsv2.decision;

import java.util.concurrent.Executor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * CP5 자동 판정({@link FraudDecisionEventListener})을 Kafka 컨슈머 스레드에서 분리하기 위한
 * 전용 스레드풀.
 *
 * 코드 리뷰 지적: {@link org.springframework.context.event.EventListener}는 별도 TaskExecutor
 * 설정이 없으면 발행자와 같은 스레드에서 동기 실행된다. CP3의 {@code AccountFeatureStoreSinkListener}가
 * Redis 쓰기 직후 이 이벤트를 발행하므로, CP5 리스너가 동기로 남아있으면 매 거래마다 TorchServe
 * 호출(최대 {@code timeout-ms})이 CP3의 Kafka 컨슈머 스레드를 그만큼 막아버려 컨슈머 랙/리밸런싱
 * 위험을 낳는다 — CLAUDE.md 설계 원칙 4번("Spring Boot가 비동기 호출")과도 어긋난다.
 *
 * <p>비동기로 옮겨도 CP5가 "레이스 컨디션을 피하려고 이벤트를 동기로 설계했다"는 전제는 깨지지
 * 않는다 — CP3가 이벤트를 발행하는 시점 자체(Redis RPUSH 완료 후)는 그대로이고, 그 이벤트를
 * "어떤 스레드가 나중에 처리하느냐"만 바뀌기 때문이다. 처리 스레드가 바뀐 시점에도 RPUSH는 이미
 * 끝나 있으므로 CP5가 읽는 시퀀스는 항상 최신이다.
 */
@Configuration
@EnableAsync
public class DecisionAsyncConfig {

    @Bean("fraudDecisionExecutor")
    public Executor fraudDecisionExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("fraud-decision-");
        executor.initialize();
        return executor;
    }
}
