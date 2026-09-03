package com.fdsv2.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * [임시 컴포넌트] 파티셔닝 검증용 컨슈머.
 *
 * 목적: "같은 accountId가 항상 같은 파티션으로 들어오는가"를 로그로 확인한다 (CP1 검증).
 *
 * Kafka Streams 토폴로지(State Store, changelog topic 등)를 바로 만들면 파티셔닝 자체의
 * 정상 동작 확인이 늦어지므로, 최소 기능으로 먼저 검증 후 교체하는 단계적 접근을 취한다.
 * 이 컨슈머는 실시간 시퀀스 집계(슬라이딩 윈도우 통계) 로직을 담당하지 않으며,
 * Kafka Streams 슬라이딩 윈도우 집계 토폴로지(CP2)가 구현되면 완전히 교체될 예정이다
 * (docs/BACKEND.md 핵심 설계 결정 3번 참고).
 */
@Slf4j
@Component
public class TransactionEventConsumer {

    @KafkaListener(
            topics = "${fds.kafka.transaction-events.topic-name}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(ConsumerRecord<String, TransactionEvent> record) {
        log.info("거래 이벤트 수신: accountId={}, partition={}, offset={}",
                record.key(), record.partition(), record.offset());
    }
}
