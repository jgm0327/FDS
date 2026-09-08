package com.fdsv2.featurestore;

import java.time.Instant;

/**
 * CP3가 계좌 피처(스냅샷 SET + 최근 시퀀스 RPUSH) 갱신을 마쳤을 때 발행하는 Spring 애플리케이션
 * 이벤트.
 *
 * CP3는 이 이벤트를 누가 구독하는지 몰라도 된다 — 지금은 CP5(판정 및 대응,
 * backend/decision-ensemble)의 {@code FraudDecisionEventListener}가 구독해서 자동 판정을
 * 트리거하지만, 나중에 다른 소비자(알림, 감사 로그 등)가 추가돼도 이 클래스와
 * {@link AccountFeatureStoreSinkListener}는 다시 바꿀 필요가 없다.
 *
 * <p>Spring의 {@code ApplicationEventPublisher}는 별도 TaskExecutor 설정이 없는 한 발행자와 같은
 * 스레드에서 동기 호출한다 — Redis 쓰기(SET + RPUSH + TRIM + EXPIRE)가 전부 끝난 "이후"에 발행하면,
 * 구독자는 항상 이번 거래까지 반영된 최신 상태를 보게 된다. CP5가 같은 토픽을 별도 Kafka 컨슈머
 * 그룹으로 다시 구독하는 대신 이 이벤트를 선택한 핵심 이유(레이스 컨디션 회피)는
 * {@code com.fdsv2.decision.FraudDecisionEventListener} 클래스 javadoc과 세션 로그 참고.
 */
public record FeatureStoreUpdatedEvent(String accountId, String featureJson, Instant occurredAt) {
}
