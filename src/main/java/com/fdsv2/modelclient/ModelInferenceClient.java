package com.fdsv2.modelclient;

/**
 * CP4 모델 서빙 호출 인터페이스 (docs/ARCHITECTURE.md 4번).
 *
 * 타임아웃/장애 시에도 항상 어떤 스코어든 반환한다 — 절대 예외를 던지지 않는다. 실패 시
 * 규칙 기반 폴백 스코어로 전환하는 것까지가 이 인터페이스의 계약이다(Circuit Breaker 연계).
 */
public interface ModelInferenceClient {

    FraudScore predict(String accountId);
}
