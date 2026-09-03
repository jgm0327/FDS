package com.fdsv2.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * transaction-events 토픽 정의.
 *
 * 파티션 수/복제 계수는 application.yml(fds.kafka.transaction-events.*)에서 관리한다.
 * 파티션 수는 운영 중 늘리면 accountId-파티션 매핑이 깨지므로, 트래픽이 없는 지금 단계에서
 * "넉넉하게 시작 → k6 실측 후 조정" 원칙에 따라 초기값 32로 잡았다 (docs/BACKEND.md 핵심 설계 결정 1번 참고).
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${fds.kafka.transaction-events.topic-name}")
    private String topicName;

    @Value("${fds.kafka.transaction-events.partitions}")
    private int partitions;

    @Value("${fds.kafka.transaction-events.replication-factor}")
    private short replicationFactor;

    @Bean
    public NewTopic transactionEventsTopic() {
        return TopicBuilder.name(topicName)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }
}
