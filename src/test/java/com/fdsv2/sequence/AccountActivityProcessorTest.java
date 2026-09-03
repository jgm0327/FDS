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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * SequenceAggregationTopologyConfig.buildTopology(...)를 그대로 호출해서 검증한다 — 배선을 여기서
 * 따로 베껴 쓰면 운영 코드가 바뀌어도 테스트가 못 잡아내는 문제가 있어서(코드 리뷰 지적), 프로덕션
 * @Bean 메서드가 위임하는 것과 동일한 static 메서드를 재사용한다.
 *
 * 브로커/Spring 컨텍스트 없이 TopologyTestDriver로 빠르게(수 초 이내) 돌아간다 — CP1의 k6+실제
 * 브로커 검증과는 다른, 로직 자체에 대한 빠른 회귀 테스트 역할.
 */
class AccountActivityProcessorTest {

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
        assertThat(first.recentWindowCount()).isEqualTo(1);
        assertThat(first.amountRatio()).isEqualTo(1.0); // 첫 거래는 비교 대상 없음
        assertThat(first.lastTxGapSec()).isNull();
        assertThat(first.countryChanged()).isFalse();

        AccountFeatureVector second = results.get(1);
        assertThat(second.recentWindowCount()).isEqualTo(2);
        assertThat(second.amountRatio()).isEqualTo(3.0); // 300 / (직전까지 평균 100)
        assertThat(second.lastTxGapSec()).isEqualTo(30L);
        assertThat(second.countryChanged()).isFalse();

        AccountFeatureVector third = results.get(2);
        assertThat(third.recentWindowCount()).isEqualTo(3);
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
        assertThat(results.get(3).recentWindowCount()).isEqualTo(3);
    }

    @Test
    void 서로_다른_계좌의_상태는_섞이지_않는다() {
        input.pipeInput("acc-1", event("acc-1", "100", "KR", T0));
        input.pipeInput("acc-2", event("acc-2", "999", "US", T0));
        input.pipeInput("acc-1", event("acc-1", "100", "KR", T0.plusSeconds(10)));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results.get(0).recentWindowCount()).isEqualTo(1); // acc-1 첫 거래
        assertThat(results.get(1).recentWindowCount()).isEqualTo(1); // acc-2 첫 거래 — acc-1과 무관
        assertThat(results.get(2).recentWindowCount()).isEqualTo(2); // acc-1 두 번째 거래
    }

    @Test
    void 역전된_이벤트가_와도_이후_정상_이벤트의_경과시간_계산이_오염되지_않는다() {
        // 코드 리뷰에서 TopologyTestDriver로 재현된 버그: 역전 이벤트가 lastTransactionTimestamp를
        // 덮어써서, 그 다음 "정상" 이벤트의 gap까지 잘못 계산되던 문제.
        input.pipeInput("acc-3", event("acc-3", "10", "KR", T0.plusSeconds(100))); // 기준
        input.pipeInput("acc-3", event("acc-3", "10", "KR", T0)); // 역전된(더 이른) 이벤트
        input.pipeInput("acc-3", event("acc-3", "10", "KR", T0.plusSeconds(130))); // 다시 정상 진행

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results).hasSize(3);
        // 역전 이벤트(2번째)가 lastTransactionTimestamp를 덮어쓰지 않았다면, 3번째 이벤트의 gap은
        // 여전히 1번째(T0+100s) 기준으로 계산되어 30초가 나와야 한다. 버그가 있었다면
        // T0(2번째, 역전된 값) 기준 130초로 잘못 나온다.
        assertThat(results.get(2).lastTxGapSec()).isEqualTo(30L);
    }

    @Test
    void 역전된_이벤트로_인해_최근_건수가_영구히_부풀지_않는다() {
        // 코드 리뷰에서 재현된 버그: 앞쪽만 트리밍하면 역전된(뒤에 남은) 오래된 타임스탬프가
        // 윈도우를 절대 벗어나지 못해 recentWindowCount가 계속 부풀었다.
        input.pipeInput("acc-4", event("acc-4", "10", "KR", T0.plus(Duration.ofMinutes(10))));
        input.pipeInput("acc-4", event("acc-4", "10", "KR", T0)); // 역전된 이벤트 — 윈도우 훨씬 밖
        // 10분 이상 지난 뒤 정상 이벤트 — 앞의 두 건 모두 윈도우 밖으로 트리밍되어야 함.
        input.pipeInput("acc-4", event("acc-4", "10", "KR", T0.plus(Duration.ofMinutes(20))));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results.get(2).recentWindowCount()).isEqualTo(1);
    }

    @Test
    void occurredAt이나_amount가_없으면_예외_없이_건너뛴다() {
        input.pipeInput("acc-5", new TransactionEvent("tx-bad", "acc-5", null, "grocery", "KR", null));
        input.pipeInput("acc-5", event("acc-5", "100", "KR", T0));

        List<AccountFeatureVector> results = output.readValuesToList();
        assertThat(results).hasSize(1); // 잘못된 레코드는 건너뛰고, 다음 정상 레코드만 출력됨
        assertThat(results.get(0).recentWindowCount()).isEqualTo(1);
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
