package com.fdsv2.transaction;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 거래 이벤트 도메인 모델.
 *
 * accountId는 Kafka 파티션 키로 사용되어, 같은 계좌의 거래가 항상 같은 파티션/태스크로
 * 전달되도록 하는 시퀀스 순서 보장의 전제조건이다 (docs/ARCHITECTURE.md 1번 참고).
 */
public record TransactionEvent(
        String transactionId,
        String accountId,
        BigDecimal amount,
        String merchantCategory,
        String country,
        Instant occurredAt
) {
}
