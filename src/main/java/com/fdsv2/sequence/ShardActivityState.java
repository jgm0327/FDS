package com.fdsv2.sequence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * (backend/kafka-salting) Stage 1 — 샤드 하나가 유지하는 부분 상태.
 *
 * 고빈도 계좌는 여러 샤드("accountId#0".."accountId#N-1")로 트래픽이 나뉘어 들어오고, 샤드마다
 * 독립된 State Store 엔트리(키 = 샤드 전체 문자열)를 갖는다 — 옛 단일 계좌 상태(이번 PR에서
 * 삭제된 AccountActivityProcessor/State)와 달리 lastTransactionTimestamp/lastCountry는 여기서
 * 관리하지 않는다. 그 값은 "전역에서 가장 최근"이어야 하는데 샤드 하나만 보고는 알 수 없고,
 * Stage 2({@link AccountActivityMergeProcessor})가 여러 샤드를 병합해서 계산한다.
 */
@Data
@NoArgsConstructor
public class ShardActivityState {

    /** 이 샤드로 들어온 거래의 슬라이딩 윈도우 내 타임스탬프. */
    private Deque<Instant> recentTimestamps = new ArrayDeque<>();

    /** 이 샤드로 들어온 거래의 누적 합계/건수(전체 계좌 합계의 일부). */
    private BigDecimal totalAmount = BigDecimal.ZERO;
    private long totalCount = 0;
}
