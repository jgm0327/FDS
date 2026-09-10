package com.fdsv2.sequence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * (backend/kafka-salting) Stage 2 — 계좌 하나의 State Store 값. 그 계좌에 존재하는 각 샤드의
 * "최신으로 알려진" 부분 상태 스냅샷 + 전역(샤드 무관) 병합 상태를 함께 갖는다.
 *
 * 일반 계좌(샤드가 항상 0번 하나뿐)는 {@code shardSnapshots}에 슬롯이 하나뿐이라, 이 클래스의
 * 병합 로직이 옛 {@link AccountActivityState} 기반 단일 계좌 집계와 수학적으로 동일한 결과를
 * 낸다(AccountActivityMergeProcessorTest 참고).
 */
@Data
@NoArgsConstructor
public class AccountMergeState {

    /** 샤드 인덱스 -> 그 샤드가 마지막으로 보고한 부분 상태. */
    private Map<Integer, ShardSnapshot> shardSnapshots = new HashMap<>();

    /**
     * 전역(모든 샤드 통틀어) "지금까지 본 것 중 가장 최근" 거래 시각/국가 — 옛
     * AccountActivityProcessor와 동일한 역전 이벤트 방어 원칙(더 이른 이벤트가 오면 덮어쓰지 않음)을
     * 여러 샤드에 걸쳐 그대로 적용한다.
     */
    private Instant lastTransactionTimestamp;
    private String lastCountry;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ShardSnapshot {
        private BigDecimal totalAmount = BigDecimal.ZERO;
        private long totalCount = 0;
        private int recentWindowCount = 0;
    }
}
