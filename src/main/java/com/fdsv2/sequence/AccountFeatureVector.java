package com.fdsv2.sequence;

/**
 * CP2(실시간 시퀀스 집계) 출력 — 계좌별 피처 벡터.
 *
 * account-feature-updates 토픽으로 발행되며, CP3(Redis 피처 스토어, 별도 브랜치)가 이 형식을
 * 그대로 계좌ID 키로 저장한다 (docs/ARCHITECTURE.md 3번 예시와 필드 의미는 동일). 이 레코드가
 * CP2와 CP3 사이의 계약(contract)이므로, 필드를 바꿀 때는 CP3 쪽도 함께 고려해야 한다.
 *
 * ARCHITECTURE.md 원문 예시는 필드명이 recent_5min_count지만, 윈도우 크기가
 * fds.sequence-aggregation.recent-window-minutes로 설정 가능해서(기본값만 5분) "5분"을 이름에
 * 박아두면 다른 윈도우로 튜닝했을 때 이름과 의미가 어긋난다 — 코드 리뷰에서 지적받아
 * recentWindowCount로 바꿨다. CP3가 아직 없어 지금 바꿔도 깨지는 소비자가 없다.
 */
public record AccountFeatureVector(
        String accountId,
        /** 설정된 윈도우(기본 5분, fds.sequence-aggregation.recent-window-minutes) 내 거래 횟수 — 이번 거래 포함. */
        int recentWindowCount,
        /**
         * 이번 거래 금액 / "평소" 금액(이번 거래 이전까지의 전체 기간 단순 평균).
         * 첫 거래(비교 대상 없음)는 1.0으로 처리한다.
         */
        double amountRatio,
        /** 직전 거래 이후 경과 시간(초). 첫 거래는 null. */
        Long lastTxGapSec,
        /** 직전 거래와 국가가 다르면 true. 첫 거래는 false. */
        boolean countryChanged
) {
}
