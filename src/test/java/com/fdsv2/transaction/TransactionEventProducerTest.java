package com.fdsv2.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * (backend/kafka-salting) 일반 계좌는 항상 샤드 0으로 고정되고(파티셔닝 불변), 고빈도로 지정된
 * 계좌만 샤드가 무작위로 분산되는지 검증한다.
 */
class TransactionEventProducerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    private TransactionEvent event(String accountId) {
        return new TransactionEvent("tx-1", accountId, new BigDecimal("1000"), "grocery", "KR", Instant.now());
    }

    @SuppressWarnings("unchecked")
    private void stubSendSuccess() {
        SendResult<String, Object> sendResult = new SendResult<>(
                new ProducerRecord<>("transaction-events", "k", "v"),
                new RecordMetadata(new TopicPartition("transaction-events", 0), 0, 0, 0, 0, 0));
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));
    }

    @Test
    void 고빈도로_지정되지_않은_일반_계좌는_항상_샤드0으로_발행된다() {
        stubSendSuccess();
        TransactionEventProducer producer =
                new TransactionEventProducer(kafkaTemplate, meterRegistry, "transaction-events", "", 8);

        for (int i = 0; i < 20; i++) {
            producer.publish(event("acc-normal"));
        }

        var keyCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        org.mockito.Mockito.verify(kafkaTemplate, org.mockito.Mockito.times(20))
                .send(anyString(), keyCaptor.capture(), any());
        assertThat(keyCaptor.getAllValues()).containsOnly("acc-normal#0");
    }

    @Test
    void 고빈도로_지정된_계좌는_여러_샤드로_분산된다() {
        stubSendSuccess();
        TransactionEventProducer producer =
                new TransactionEventProducer(kafkaTemplate, meterRegistry, "transaction-events", "acc-hot", 8);

        Set<String> observedKeys = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            producer.publish(event("acc-hot"));
        }
        var keyCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        org.mockito.Mockito.verify(kafkaTemplate, org.mockito.Mockito.times(200))
                .send(anyString(), keyCaptor.capture(), any());
        observedKeys.addAll(keyCaptor.getAllValues());

        // 200번 발행하면 8개 샤드 중 확률상 전부 관측될 것이 거의 확실하다 — 최소한 "1개로 안
        // 몰린다"(분산이 실제로 일어난다)는 것만 확인한다.
        assertThat(observedKeys).hasSizeGreaterThan(1);
        assertThat(observedKeys).allMatch(key -> key.startsWith("acc-hot#"));
    }
}
