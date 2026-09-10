package com.fdsv2.sequence;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * (backend/kafka-salting) Stage 1({@link ShardedAccountActivityProcessor})의 출력이자
 * Stage 2({@link AccountActivityMergeProcessor})의 입력.
 *
 * Kafka 메시지 키를 샤드 키("accountId#N")에서 순수 accountId로 바꾸는 지점
 * ({@code SequenceAggregationTopologyConfig}의 {@code selectKey}+{@code repartition})에서
 * 값으로 실려가는 레코드 — 같은 계좌의 여러 샤드가 이 재파티션을 거쳐 다시 하나의 Streams
 * 태스크로 모인다.
 *
 * shardTotalAmount/shardTotalCount는 "이번 거래까지 포함한" 그 샤드의 누적값이다 — Stage 2가
 * 다음 병합 때 "이 샤드의 이전 스냅샷"으로 참조할 수 있도록.
 */
public record PartialAccountFeatureVector(
        String accountId,
        int shardIndex,
        int shardRecentWindowCount,
        BigDecimal shardTotalAmount,
        long shardTotalCount,
        BigDecimal amount,
        Instant occurredAt,
        String country,
        String merchantCategory) {
}
