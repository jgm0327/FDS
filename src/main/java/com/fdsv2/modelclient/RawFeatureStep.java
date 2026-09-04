package com.fdsv2.modelclient;

/**
 * {@code feature:account:{id}:recent} Redis LIST(backend/sequence-window-feature-store)에 담긴
 * 원소 하나를 역직렬화하기 위한 타입 — CP2 {@code com.fdsv2.sequence.AccountFeatureVector}와
 * 필드가 완전히 같지만, 그 자바 타입에 직접 의존하지 않고 이 패키지에서 독립적으로 정의한다.
 *
 * CP3가 "CP2 도메인 클래스에 의존하지 않고 토픽에 담긴 JSON 형태만 계약으로 삼는다"고 결정했던
 * 것과 같은 이유다 (AccountFeatureStoreSinkListener 참고) — 이 계약을 Redis LIST를 소비하는
 * 이 브랜치까지 일관되게 유지한다.
 */
public record RawFeatureStep(
        String accountId,
        int recentWindowCount,
        double amountRatio,
        Long lastTxGapSec,
        boolean countryChanged,
        String merchantCategory) {
}
