package com.fdsv2.sequence;

import com.fdsv2.transaction.TransactionEvent;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * CP2 토폴로지 배선 — transaction-events(계좌ID 키)를 읽어서, 계좌별 State Store
 * (RocksDB, {@value #STORE_NAME})로 AccountActivityProcessor가 슬라이딩 윈도우 통계를 계산하고,
 * 결과를 account-feature-updates 토픽으로 발행한다 (docs/BACKEND.md CP2 참고).
 *
 * State Store 값 직렬화는 Spring Kafka의 JsonSerde(classic Jackson 2 기반)를 쓴다 — CP1의
 * KafkaTemplate 프로듀서/컨슈머가 이미 JsonSerializer/JsonDeserializer로 이 방식을 쓰고 있어서
 * 스택 전체의 일관성을 유지하기 위한 선택이다. Spring Kafka 4.0부터 JsonSerde는
 * {@code @Deprecated(forRemoval = true)}로 표시되어 있고 Jackson 3 기반 JacksonJsonSerde로 대체될
 * 예정이지만, 지금 Jackson 3로 옮기면 classic Jackson 2(프로듀서/컨슈머 쪽)와 Jackson 3(Streams
 * 쪽) 두 스택이 동시에 클래스패스에 올라가서 얻는 것 없이 복잡도만 늘어난다 — 스택 전체를 한 번에
 * 옮기는 별도 작업으로 남겨둔다.
 */
@Configuration
@EnableKafkaStreams
public class SequenceAggregationTopologyConfig {

    private static final String STORE_NAME = "account-activity-store";

    @Value("${fds.kafka.transaction-events.topic-name}")
    private String inputTopic;

    @Value("${fds.kafka.feature-updates.topic-name}")
    private String outputTopic;

    @Value("${fds.kafka.feature-updates.partitions}")
    private int outputPartitions;

    @Value("${fds.kafka.feature-updates.replication-factor}")
    private short outputReplicationFactor;

    @Value("${fds.sequence-aggregation.recent-window-minutes}")
    private long recentWindowMinutes;

    @Bean
    public NewTopic accountFeatureUpdatesTopic() {
        return TopicBuilder.name(outputTopic)
                .partitions(outputPartitions)
                .replicas(outputReplicationFactor)
                .build();
    }

    @Bean
    public KStream<String, TransactionEvent> accountActivityStream(StreamsBuilder streamsBuilder) {
        StoreBuilder<KeyValueStore<String, AccountActivityState>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                new JsonSerde<>(AccountActivityState.class));
        // 로깅(changelog topic 백업)은 Kafka Streams 기본값으로 이미 켜져 있다 — 별도 설정 불필요
        // (docs/ARCHITECTURE.md 2번 "장애 대비" 요건).
        streamsBuilder.addStateStore(storeBuilder);

        Duration recentWindow = Duration.ofMinutes(recentWindowMinutes);

        KStream<String, TransactionEvent> transactions = streamsBuilder.stream(
                inputTopic, Consumed.with(Serdes.String(), new JsonSerde<>(TransactionEvent.class)));

        transactions
                .processValues(() -> new AccountActivityProcessor(STORE_NAME, recentWindow), STORE_NAME)
                .to(outputTopic, Produced.with(Serdes.String(), new JsonSerde<>(AccountFeatureVector.class)));

        return transactions;
    }
}
