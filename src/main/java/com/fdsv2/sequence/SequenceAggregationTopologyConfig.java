package com.fdsv2.sequence;

import com.fdsv2.transaction.TransactionEvent;
import io.micrometer.core.instrument.config.MeterFilter;
import java.time.Duration;
import java.util.Set;
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

    /** package-private로 노출 — AccountActivityProcessorTest가 프로덕션과 같은 store name을 쓰기 위함. */
    static final String STORE_NAME = "account-activity-store";

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

    /**
     * CP2 관측 확장 — Kafka Streams 자체 지표(레코드 처리 latency, RocksDB State Store 지표,
     * 리밸런싱 등)를 Prometheus/Grafana로 노출한다 (docs/PERFORMANCE_MEASUREMENT.md CP2 표 참고).
     *
     * Spring Boot 4.1.1은 micrometer-core + StreamsBuilderFactoryBean이 클래스패스에 있으면
     * {@code KafkaMetricsAutoConfiguration.KafkaStreamsMetricsConfiguration}이 이미
     * {@code StreamsBuilderFactoryBeanConfigurer}로 {@code KafkaStreamsMicrometerListener}를
     * 자동 등록해준다 (jar 바이트코드로 직접 확인함) — 그래서 별도로 빈을 만들 필요가 없다. 처음엔
     * 몰라서 직접 만들었었는데, 코드 리뷰에서 "Boot가 이미 하는 일을 중복 등록하고 있다"는 지적을
     * 받고 나서야 확인했다. 여기서 실제로 필요한 건 아래 필터뿐이다 — RocksDB 지표까지 잡히게 하려면
     * metrics.recording.level을 DEBUG로 올려야 하는데, 그건 application.yml에서 설정한다.
     */

    /**
     * Boot가 자동으로 붙여주는 리스너는 kafkaStreams.metrics()에 있는 raw Kafka 지표(수백 개, 대부분
     * CP2와 무관한 내부 컨슈머/프로듀서/어드민 클라이언트 지표)를 전부 기계적으로 바인딩한다. 실측
     * 중 이 중 최소 2개가 이름과 달리 음수 값을 내는 지표(예: restore-remaining-records-total —
     * "누적 카운터"처럼 이름 붙었지만 실제로는 복구가 진행되며 줄어드는 값이라 restore 중이 아닌
     * 태스크에서 -1 sentinel을 냄)라서 `/actuator/prometheus` 전체가
     * "counters cannot have a negative value" 예외로 500이 나는 걸 두 번 확인했다.
     *
     * 처음엔 "이름이 .total로 끝나는 지표만 카운터로 안 믿는다"는 블랙리스트 규칙을 시도했는데,
     * 부하 테스트를 다시 돌리자 그 규칙도 못 잡는 또 다른 음수 지표가 나왔다 — 어떤 지표가 언제
     * 음수를 낼지 미리 다 알 수 없다는 뜻이라, 문제가 생길 때마다 하나씩 막는 블랙리스트 방식
     * 자체를 포기했다.
     *
     * 대신 화이트리스트로 전환: CP2 대시보드가 실제로 필요로 하는 지표(레코드 처리 latency, RocksDB
     * put/get/e2e latency, restore latency, 리밸런싱 latency)만 이름으로 명시적으로 허용하고,
     * kafka.stream./kafka.consumer./kafka.admin./kafka.producer. 아래 나머지는 전부 차단한다.
     * kafka.producer.*도 포함시킨 이유: metrics.recording.level=DEBUG는 Kafka Streams가 내부적으로
     * 쓰는 프로듀서(changelog/출력 토픽에 쓰는 용도)의 기록 레벨도 함께 올리므로, 컨슈머/어드민과
     * 똑같이 이름만 보고 오분류된 음수 카운터가 나올 위험이 있다 (코드 리뷰에서 지적) — 실제로
     * 크래시가 재현된 적은 없지만, 이 필터가 막으려는 문제 자체가 "이름만 보고는 알 수 없다"는
     * 것이므로 미리 막아둔다.
     *
     * "Meter.Id.getTag("spring.id") == "defaultKafkaStreamsBuilder""로 범위를 Kafka Streams가 만든
     * 클라이언트로만 한정한 이유: 이름 접두사만으로 걸면 나중에 이 앱에 다른 순수 Kafka 컨슈머나
     * AdminClient(예: CP1에 있었던 것 같은 @KafkaListener)가 다시 생겼을 때 그쪽 지표까지 이름이
     * 겹친다는 이유로 영원히 안 보이게 될 수 있다 (코드 리뷰에서 지적) — 태그로 한정하면 그런
     * 컴포넌트의 지표는 이 필터의 영향을 받지 않는다.
     */
    private static final String KAFKA_STREAMS_SPRING_ID_TAG = "defaultKafkaStreamsBuilder";

    private static final Set<String> ALLOWED_KAFKA_STREAMS_METRICS = Set.of(
            "kafka.stream.thread.process.latency.avg",
            "kafka.stream.thread.process.latency.max",
            "kafka.stream.thread.process.rate",
            "kafka.stream.thread.commit.latency.avg",
            "kafka.stream.thread.commit.latency.max",
            "kafka.stream.thread.poll.latency.avg",
            "kafka.stream.thread.poll.latency.max",
            "kafka.stream.state.put.latency.avg",
            "kafka.stream.state.put.latency.max",
            "kafka.stream.state.get.latency.avg",
            "kafka.stream.state.get.latency.max",
            "kafka.stream.state.record.e2e.latency.avg",
            "kafka.stream.state.record.e2e.latency.max",
            "kafka.stream.state.restore.latency.avg",
            "kafka.stream.state.restore.latency.max",
            "kafka.stream.state.restore.rate",
            "kafka.stream.state.updater.active.restoring.tasks",
            "kafka.stream.state.updater.standby.updating.tasks",
            "kafka.stream.alive.stream.threads",
            "kafka.consumer.coordinator.rebalance.latency.avg",
            "kafka.consumer.coordinator.rebalance.latency.max",
            "kafka.consumer.coordinator.rebalance.rate.per.hour",
            "kafka.consumer.coordinator.last.rebalance.seconds.ago");

    @Bean
    public MeterFilter kafkaStreamsMetricsAllowlistFilter() {
        return MeterFilter.denyUnless(SequenceAggregationTopologyConfig::isAllowedMetric);
    }

    /** package-private로 노출 — 단위 테스트에서 화이트리스트 로직 자체를 검증하기 위함. */
    static boolean isAllowedMetric(io.micrometer.core.instrument.Meter.Id id) {
        String name = id.getName();
        boolean isKafkaStreamsInternalClientMetric = KAFKA_STREAMS_SPRING_ID_TAG.equals(id.getTag("spring.id"))
                && (name.startsWith("kafka.stream.")
                        || name.startsWith("kafka.consumer.")
                        || name.startsWith("kafka.admin.")
                        || name.startsWith("kafka.producer."));
        return !isKafkaStreamsInternalClientMetric || ALLOWED_KAFKA_STREAMS_METRICS.contains(name);
    }

    @Bean
    public KStream<String, TransactionEvent> accountActivityStream(StreamsBuilder streamsBuilder) {
        return buildTopology(streamsBuilder, inputTopic, outputTopic, Duration.ofMinutes(recentWindowMinutes));
    }

    /**
     * 실제 토폴로지 배선 로직. static으로 뽑아둔 이유는 AccountActivityProcessorTest가 이 메서드를
     * 그대로 호출해서 검증하기 위함이다 — 테스트가 배선을 따로 베껴 쓰면 운영 코드가 바뀌어도
     * 테스트가 그걸 못 잡아내는 채로 계속 통과하는 문제가 있었다 (코드 리뷰에서 지적됨).
     */
    static KStream<String, TransactionEvent> buildTopology(
            StreamsBuilder streamsBuilder, String inputTopic, String outputTopic, Duration recentWindow) {
        StoreBuilder<KeyValueStore<String, AccountActivityState>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                new JsonSerde<>(AccountActivityState.class));
        // 로깅(changelog topic 백업)은 Kafka Streams 기본값으로 이미 켜져 있다 — 별도 설정 불필요
        // (docs/ARCHITECTURE.md 2번 "장애 대비" 요건).
        streamsBuilder.addStateStore(storeBuilder);

        KStream<String, TransactionEvent> transactions = streamsBuilder.stream(
                inputTopic, Consumed.with(Serdes.String(), new JsonSerde<>(TransactionEvent.class)));

        transactions
                .processValues(() -> new AccountActivityProcessor(STORE_NAME, recentWindow), STORE_NAME)
                .to(outputTopic, Produced.with(Serdes.String(), new JsonSerde<>(AccountFeatureVector.class)));

        return transactions;
    }
}
