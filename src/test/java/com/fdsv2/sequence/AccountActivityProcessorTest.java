package com.fdsv2.sequence;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdsv2.transaction.TransactionEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * SequenceAggregationTopologyConfig가 구성하는 토폴로지를 TopologyTestDriver로 검증한다.
 * 브로커/Spring 컨텍스트 없이 토폴로지 구성을 그대로 재현해서 빠르게(수 초 이내) 돌아간다 —
 * CP1의 k6+실제 브로커 검증과는 다른, 로직 자체에 대한 빠른 회귀 테스트 역할.
 */
class AccountActivityProcessorTest {

    private static final String STORE_NAME = "account-activity-store";
    private static final String INPUT_TOPIC = "transaction-events";
    private static final String OUTPUT_TOPIC = "account-feature-updates";
    private static final Instant T0 = Instant.parse("2026-09-03T00:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, TransactionEvent> input;
    private TestOutputTopic<String, AccountFeatureVector> output;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, AccountActivityState>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                new JsonSerde<>(AccountActivityState.class));
        builder.addStateStore(storeBuilder);

        KStream<String, TransactionEvent> stream = builder.stream(
                INPUT_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(TransactionEvent.class)));
        stream.processValues(() -> new AccountActivityProcessor(STORE_NAME, Duration.ofMinutes(5)), STORE_NAME)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(AccountFeatureVector.class)));

        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "build/kafka-streams-test-state");

        driver = new TopologyTestDriver(topology, props);
        input = driver.createInputTopic(INPUT_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(TransactionEvent.class).serializer());
        output = driver.createOutputTopic(OUTPUT_TOPIC, Serdes.String().deserializer(),
                new JsonSerde<>(AccountFeatureVector.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void 계좌별_슬라이딩윈도우_통계를_순서대로_계산한다() {
        input.pipeInput("acc-1", event("acc-1", "100", "KR", T0));
        input.pipeInput("acc-1", event("acc-1", "300", "KR", T0.plusSeconds(30)));
        input.pipeInput("acc-1", event("acc-1", "50", "US", T0.plusSeconds(90)));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results).hasSize(3);

        AccountFeatureVector first = results.get(0);
        assertThat(first.recent5MinCount()).isEqualTo(1);
        assertThat(first.amountRatio()).isEqualTo(1.0); // 첫 거래는 비교 대상 없음
        assertThat(first.lastTxGapSec()).isNull();
        assertThat(first.countryChanged()).isFalse();

        AccountFeatureVector second = results.get(1);
        assertThat(second.recent5MinCount()).isEqualTo(2);
        assertThat(second.amountRatio()).isEqualTo(3.0); // 300 / (직전까지 평균 100)
        assertThat(second.lastTxGapSec()).isEqualTo(30L);
        assertThat(second.countryChanged()).isFalse();

        AccountFeatureVector third = results.get(2);
        assertThat(third.recent5MinCount()).isEqualTo(3);
        assertThat(third.amountRatio()).isEqualTo(0.25); // 50 / (직전까지 평균 (100+300)/2=200)
        assertThat(third.lastTxGapSec()).isEqualTo(60L);
        assertThat(third.countryChanged()).isTrue(); // KR -> US
    }

    @Test
    void 윈도우를_벗어난_과거_거래는_최근_건수에서_제외된다() {
        input.pipeInput("acc-2", event("acc-2", "10", "KR", T0));
        input.pipeInput("acc-2", event("acc-2", "10", "KR", T0.plus(Duration.ofMinutes(2))));
        input.pipeInput("acc-2", event("acc-2", "10", "KR", T0.plus(Duration.ofMinutes(4))));
        input.pipeInput("acc-2", event("acc-2", "10", "KR", T0.plus(Duration.ofMinutes(7))));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results).hasSize(4);
        // 7분 시점 기준 최근 5분 윈도우는 [2분, 7분] — 0분 거래는 윈도우를 벗어나 트리밍된다.
        assertThat(results.get(3).recent5MinCount()).isEqualTo(3);
    }

    @Test
    void 서로_다른_계좌의_상태는_섞이지_않는다() {
        input.pipeInput("acc-1", event("acc-1", "100", "KR", T0));
        input.pipeInput("acc-2", event("acc-2", "999", "US", T0));
        input.pipeInput("acc-1", event("acc-1", "100", "KR", T0.plusSeconds(10)));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results.get(0).recent5MinCount()).isEqualTo(1); // acc-1 첫 거래
        assertThat(results.get(1).recent5MinCount()).isEqualTo(1); // acc-2 첫 거래 — acc-1과 무관
        assertThat(results.get(2).recent5MinCount()).isEqualTo(2); // acc-1 두 번째 거래
    }

    private static TransactionEvent event(String accountId, String amount, String country, Instant occurredAt) {
        return new TransactionEvent(
                "tx-" + accountId + "-" + occurredAt.toEpochMilli(),
                accountId,
                new BigDecimal(amount),
                "grocery",
                country,
                occurredAt);
    }
}
