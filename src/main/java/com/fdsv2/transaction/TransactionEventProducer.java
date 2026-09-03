package com.fdsv2.transaction;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * transaction-events 토픽 발행 담당.
 *
 * accountId를 메시지 key로 명시하는 것이 파티셔닝 전략의 핵심 전제조건이다 — 같은 계좌의
 * 이벤트가 항상 같은 파티션(같은 Kafka Streams 태스크)으로 가야 시퀀스 순서가 보장된다
 * (docs/ARCHITECTURE.md 1번 참고).
 *
 * 프로듀서 신뢰성 설정(idempotence, acks=all, max.in.flight=5)은 application.yml에서 관리하며,
 * 재시도 상황에서도 파티션 내 순서가 깨지지 않도록 하는 Kafka 공식 권장 조합이다
 * (docs/BACKEND.md 핵심 설계 결정 2번 참고).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${fds.kafka.transaction-events.topic-name}")
    private String topicName;

    public void publish(TransactionEvent event) {
        kafkaTemplate.send(topicName, event.accountId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("거래 이벤트 발행 실패: accountId={}, transactionId={}",
                                event.accountId(), event.transactionId(), ex);
                        return;
                    }
                    var metadata = result.getRecordMetadata();
                    log.info("거래 이벤트 발행 성공: accountId={}, transactionId={}, partition={}, offset={}",
                            event.accountId(), event.transactionId(), metadata.partition(), metadata.offset());
                });
    }
}
