package com.fdsv2.sequence;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.junit.jupiter.api.Test;

/**
 * kafkaStreamsMetricsAllowlistFilter가 실제로 어떤 지표를 통과/차단시키는지 검증한다.
 * 코드 리뷰 지적: 이 화이트리스트는 사람이 손으로 적은 문자열 목록이라, 나중에 Kafka/Micrometer
 * 버전이 올라가면서 지표 이름이 바뀌면 아무 실패 없이 조용히 데이터가 사라질 수 있다 — 적어도
 * "지금 시점에 어떤 이름이 허용/차단되는지"는 테스트로 고정해서, 필터 로직 자체의 회귀는 잡는다.
 */
class SequenceAggregationTopologyConfigTest {

    private static final Tag STREAMS_SPRING_ID = Tag.of("spring.id", "defaultKafkaStreamsBuilder");

    @Test
    void 화이트리스트에_있는_kafka_streams_지표는_허용된다() {
        Meter.Id id = new Meter.Id("kafka.stream.state.put.latency.avg", Tags.of(STREAMS_SPRING_ID),
                null, null, Meter.Type.GAUGE);
        assertThat(SequenceAggregationTopologyConfig.isAllowedMetric(id)).isTrue();
    }

    @Test
    void 화이트리스트에_없는_kafka_streams_지표는_차단된다() {
        // 실측 중 음수 값으로 /actuator/prometheus를 500 나게 했던 바로 그 지표.
        Meter.Id id = new Meter.Id("kafka.stream.task.restore.remaining.records.total",
                Tags.of(STREAMS_SPRING_ID), null, null, Meter.Type.COUNTER);
        assertThat(SequenceAggregationTopologyConfig.isAllowedMetric(id)).isFalse();
    }

    @Test
    void metrics_recording_level_DEBUG로_인해_같이_올라가는_내부_프로듀서_지표도_차단된다() {
        // Kafka Streams가 changelog/출력 토픽에 쓸 때 쓰는 내부 프로듀서 지표 — 코드 리뷰에서
        // "컨슈머/어드민만 막고 프로듀서는 안 막아서 같은 버그가 재발할 수 있다"고 지적받아 추가.
        Meter.Id id = new Meter.Id("kafka.producer.some.unlisted.total", Tags.of(STREAMS_SPRING_ID),
                null, null, Meter.Type.COUNTER);
        assertThat(SequenceAggregationTopologyConfig.isAllowedMetric(id)).isFalse();
    }

    @Test
    void spring_id_태그가_다른_클라이언트의_지표는_이름이_겹쳐도_필터에_안_걸린다() {
        // 나중에 이 앱에 순수 Kafka 컨슈머(예: 또 다른 @KafkaListener)가 다시 생겼을 때, 그
        // 컴포넌트의 지표까지 이름이 우연히 겹친다는 이유로 영원히 안 보이게 되면 안 된다.
        Meter.Id id = new Meter.Id("kafka.consumer.coordinator.rebalance.total",
                Tags.of(Tag.of("spring.id", "someOtherConsumerFactory")), null, null, Meter.Type.COUNTER);
        assertThat(SequenceAggregationTopologyConfig.isAllowedMetric(id)).isTrue();
    }

    @Test
    void kafka와_무관한_지표는_영향받지_않는다() {
        Meter.Id id = new Meter.Id("jvm.memory.used", Tags.empty(), null, null, Meter.Type.GAUGE);
        assertThat(SequenceAggregationTopologyConfig.isAllowedMetric(id)).isTrue();
    }
}
