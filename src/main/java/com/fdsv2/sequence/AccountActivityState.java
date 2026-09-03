package com.fdsv2.sequence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CP2 State Store(RocksDB, 계좌ID 키)에 저장되는 계좌별 누적 상태.
 *
 * AccountActivityProcessor가 매 거래마다 이 상태를 읽고 → AccountFeatureVector를 계산하고 →
 * 갱신된 상태를 다시 저장한다. State Store 변경은 Kafka Streams가 기본으로 changelog topic에
 * 백업하므로(docs/ARCHITECTURE.md 2번 "장애 대비" 참고), 태스크가 죽어도 다른 인스턴스가 이
 * changelog를 replay해서 복원할 수 있다.
 *
 * Jackson(classic Jackson 2, JsonSerde 경유)으로 직렬화되므로 기본 생성자 + getter/setter가 필요하다
 * (Lombok @Data/@NoArgsConstructor로 생성).
 */
@Data
@NoArgsConstructor
public class AccountActivityState {

    /** 슬라이딩 윈도우 내 거래 타임스탬프. 오래된 항목은 매 거래마다 트리밍한다. */
    private Deque<Instant> recentTimestamps = new ArrayDeque<>();

    /** "평소 금액" 계산을 위한 전체 기간 누적 합계/건수 (all-time simple moving average). */
    private BigDecimal totalAmount = BigDecimal.ZERO;
    private long totalCount = 0;

    /** 직전 거래 시각/국가 — null이면 "이번이 첫 거래"라는 뜻. */
    private Instant lastTransactionTimestamp;
    private String lastCountry;
}
