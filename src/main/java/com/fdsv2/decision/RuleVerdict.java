package com.fdsv2.decision;

import java.util.List;

/**
 * {@link RuleEngine#evaluate}의 결과.
 *
 * override가 null이 아니면 화이트리스트/하드룰로 이미 최종 액션이 정해진 것이다 — 이 경우
 * {@link EnsembleFraudDecisionService}는 앙상블 계산도, 모델 호출도 건너뛴다("모델 확률과 무관하게
 * 즉시 반영", docs/ARCHITECTURE.md 5번). override가 null이면 ruleScore만 앙상블 입력으로 쓰인다.
 */
public record RuleVerdict(Action override, double ruleScore, List<String> triggeredRules) {
}
