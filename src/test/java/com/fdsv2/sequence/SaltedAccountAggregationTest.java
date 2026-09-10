package com.fdsv2.sequence;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdsv2.transaction.AccountShardKey;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * (backend/kafka-salting) 여러 샤드로 들어오는 고빈도 계좌의 거래가 Stage 1(샤드별 병렬 집계) +
 * Stage 2(계좌 단위 재집계)를 거쳐 올바르게 병합되는지 검증한다.
 *
 * {@link AccountActivityProcessorTest}(옛 이름 그대로 유지)가 "샤드 1개(일반 계좌)는 기존과
 * 완전히 동일하다"를 검증한다면, 이 테스트는 "샤드가 여러 개일 때 병합이 실제로 의미 있게
 * 동작하는가"를 검증한다.
 */
class SaltedAccountAggregationTest {

    private static final String INPUT_TOPIC = "transaction-events";
    private static final String OUTPUT_TOPIC = "account-feature-updates";
    private static final Instant T0 = Instant.parse("2026-09-03T00:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, TransactionEvent> input;
    private TestOutputTopic<String, AccountFeatureVector> output;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        SequenceAggregationTopologyConfig.buildTopology(builder, INPUT_TOPIC, OUTPUT_TOPIC, Duration.ofMinutes(5));
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app-salting");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "build/kafka-streams-test-state-salting");

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
    void 서로_다른_샤드로_들어온_같은_계좌_거래의_recentWindowCount는_샤드_합산이다() {
        // acc-hot이 3개 샤드에 나뉘어 들어온다 — 실제 프로듀서는 무작위로 나누지만, 테스트는
        // 결정적으로 각 샤드에 1건씩 보내서 "3건 = 합계 3"을 검증한다.
        input.pipeInput(AccountShardKey.format("acc-hot", 0), event("acc-hot", "100", "KR", T0));
        input.pipeInput(AccountShardKey.format("acc-hot", 1), event("acc-hot", "100", "KR", T0.plusSeconds(1)));
        input.pipeInput(AccountShardKey.format("acc-hot", 2), event("acc-hot", "100", "KR", T0.plusSeconds(2)));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results).hasSize(3);
        // 세 번째(마지막) 이벤트 시점에는 3개 샤드 모두 자기 샤드 안에서 1건씩 봤고, 그걸 합치면 3.
        assertThat(results.get(2).recentWindowCount()).isEqualTo(3);
        assertThat(results.get(2).accountId()).isEqualTo("acc-hot");
    }

    @Test
    void amountRatio는_샤드를_넘나드는_전역_평균을_기준으로_계산된다() {
        // 샤드 0에 100, 100을 넣어 "샤드 0만 보면 평소 100"이라는 착시를 만들고, 샤드 1에 400을
        // 넣었을 때 전역 평균(0+0번째 두 건 200 / 2건 = 100)을 기준으로 4.0배가 나오는지 검증한다.
        // (샤드가 분리돼 있다고 각 샤드가 "자기만의 평소 금액"을 따로 갖고 착각하면 안 된다는 걸
        // 확인하는 테스트.)
        input.pipeInput(AccountShardKey.format("acc-ratio", 0), event("acc-ratio", "100", "KR", T0));
        input.pipeInput(AccountShardKey.format("acc-ratio", 0), event("acc-ratio", "100", "KR", T0.plusSeconds(10)));
        input.pipeInput(AccountShardKey.format("acc-ratio", 1), event("acc-ratio", "400", "KR", T0.plusSeconds(20)));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results).hasSize(3);
        // 세 번째 거래(샤드 1) 시점의 "이전까지 전역 평균" = (100+100)/2 = 100. 400/100 = 4.0.
        assertThat(results.get(2).amountRatio()).isEqualTo(4.0);
    }

    @Test
    void gap과_countryChanged는_다른_샤드의_가장_최근_거래를_기준으로_계산된다() {
        // 샤드 0에서 첫 거래(KR), 30초 뒤 샤드 1에서 두 번째 거래(US)가 온다 — 서로 다른 샤드지만
        // "직전 거래"를 정확히 인식해서 gap=30, countryChanged=true가 나와야 한다.
        input.pipeInput(AccountShardKey.format("acc-gap", 0), event("acc-gap", "10", "KR", T0));
        input.pipeInput(AccountShardKey.format("acc-gap", 1), event("acc-gap", "10", "US", T0.plusSeconds(30)));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results).hasSize(2);
        assertThat(results.get(0).lastTxGapSec()).isNull(); // 계좌 전체의 첫 거래
        assertThat(results.get(1).lastTxGapSec()).isEqualTo(30L);
        assertThat(results.get(1).countryChanged()).isTrue();
    }

    @Test
    void 서로_다른_계좌의_샤드_상태는_섞이지_않는다() {
        input.pipeInput(AccountShardKey.format("acc-a", 0), event("acc-a", "10", "KR", T0));
        input.pipeInput(AccountShardKey.format("acc-b", 0), event("acc-b", "10", "KR", T0));
        input.pipeInput(AccountShardKey.format("acc-a", 1), event("acc-a", "10", "KR", T0.plusSeconds(1)));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results.get(0).accountId()).isEqualTo("acc-a");
        assertThat(results.get(0).recentWindowCount()).isEqualTo(1);
        assertThat(results.get(1).accountId()).isEqualTo("acc-b");
        assertThat(results.get(1).recentWindowCount()).isEqualTo(1); // acc-a 샤드와 무관
        assertThat(results.get(2).accountId()).isEqualTo("acc-a");
        assertThat(results.get(2).recentWindowCount()).isEqualTo(2); // acc-a의 두 샤드 합
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
