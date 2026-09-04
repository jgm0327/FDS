package com.fdsv2.featurestore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * CP3 — Redis 쓰기 실패에 대한 재시도 정책 (docs/BACKEND.md CP3 참고, 코드 리뷰에서 지적됨).
 *
 * 처음 구현엔 에러 처리가 전혀 없었다 — Redis가 잠깐 끊기면 그 사이 도착한 레코드는 기본 동작
 * (사실상 재시도 없이 바로 offset 커밋)으로 조용히 유실됐다. {@link DefaultErrorHandler}는 Spring
 * Boot가 {@code ConcurrentKafkaListenerContainerFactoryConfigurer}를 통해 이 빈을 자동으로
 * 기본 리스너 컨테이너 팩토리에 연결해준다 — 별도 팩토리 빈을 새로 만들 필요 없음.
 *
 * 1초 간격으로 3번 재시도 후에도 실패하면(Redis가 그 이상 오래 다운된 경우) 로그만 남기고 다음
 * 레코드로 넘어간다 — Dead Letter Topic까지는 이번 범위에서 만들지 않았다 (다음 개선 후보,
 * docs/BACKEND.md 참고). 그래도 "조용히 사라짐"보다는 "실패했다는 로그가 남음"이 훨씬 낫다.
 */
@Slf4j
@Configuration
public class FeatureStoreKafkaConfig {

    @Bean
    public DefaultErrorHandler featureStoreErrorHandler() {
        return new DefaultErrorHandler(
                (record, exception) -> log.error(
                        "Redis 피처 스토어 쓰기 실패 — 재시도 소진, 이 레코드는 건너뜀: "
                                + "topic={}, partition={}, offset={}, key={}",
                        record.topic(), record.partition(), record.offset(), record.key(), exception),
                new FixedBackOff(1000L, 3));
    }
}
