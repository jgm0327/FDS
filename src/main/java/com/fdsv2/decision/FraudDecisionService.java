package com.fdsv2.decision;

import com.fdsv2.modelclient.RawFeatureStep;

/**
 * CP5 판정 엔진 (docs/ARCHITECTURE.md 5번) — 규칙 엔진 + 모델 확률 앙상블로 최종 액션을 정한다.
 * 절대 예외를 던지지 않을 필요는 없다({@link com.fdsv2.modelclient.ModelInferenceClient}와 달리) —
 * 호출부(FraudDecisionEventListener/FraudDecisionController)가 각자 자기 책임 범위에서 예외를
 * 격리한다.
 */
public interface FraudDecisionService {

    FraudDecision decide(String accountId, RawFeatureStep latestStep);
}
