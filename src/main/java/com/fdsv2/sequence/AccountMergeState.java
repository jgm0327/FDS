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
 * 병합 로직이 옛 단일 계좌 집계(session-01 이전의 AccountActivityProcessor/State, 이번 PR에서
 * 삭제됨)와 수학적으로 동일한 결과를 낸다(SaltedAccountAggregationTest, 그리고 옛
 * AccountActivityProcessorTest가 무변경으로 통과하는 것으로 회귀 검증).
 *
 * <p><b>코드 리뷰 반영 — 샤드 스냅숏의 시각(lastEventTime)을 추가한 이유</b>: 처음 구현에서는
 * {@code recentWindowCount}를 "각 샤드가 마지막으로 보고한 값의 합"으로만 계산했는데, 트래픽이
 * 끊긴 샤드의 옛 스냅숏이 영원히 합계에 남아 계좌 전체의 windowCount를 계속 부풀리는 버그가
 * 있었다(리뷰가 구체적 재현 시나리오로 지적함). {@link ShardSnapshot#lastEventTime}을 추가해서,
 * 병합 시점에 "그 샤드가 마지막으로 보고한 시각이 지금 윈도우 안인지"를 확인하고, 윈도우 밖이면
 * 그 샤드의 기여분을 0으로 취급한다({@link AccountActivityMergeProcessor} 참고). totalAmount/
 * totalCount는 전체 기간 누적값이라 이 문제가 없다 — 윈도우가 있는 recentWindowCount만의 문제.
 */
@Data
@NoArgsConstructor
public class AccountMergeState {

    /** 샤드 인덱스 -> 그 샤드가 마지막으로 보고한 부분 상태. */
    private Map<Integer, ShardSnapshot> shardSnapshots = new HashMap<>();

    /**
     * 전역(모든 샤드 통틀어) "지금까지 본 것 중 가장 최근" 거래 시각/국가 — 옛 단일 계좌 집계와
     * 동일한 역전 이벤트 방어 원칙(더 이른 이벤트가 오면 덮어쓰지 않음)을 여러 샤드에 걸쳐 그대로
     * 적용한다.
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
        /** 이 샤드가 마지막으로 이벤트를 보고한 시각 — 윈도우 밖으로 벗어났는지 판단하는 기준. */
        private Instant lastEventTime;
    }
}
