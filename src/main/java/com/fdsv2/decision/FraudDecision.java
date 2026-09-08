package com.fdsv2.decision;

import java.time.Instant;
import java.util.List;

/**
 * CP5 최종 판정 결과 (docs/ARCHITECTURE.md 5번).
 *
 * modelProbability/modelSource는 화이트리스트/하드룰로 즉시 결정된 경우 null이다 — "모델 확률과
 * 무관하게 즉시 반영"되는 케이스라 모델을 아예 호출하지 않기 때문이다(RuleEngine,
 * EnsembleFraudDecisionService 참고).
 *
 * <p>이 레코드를 그대로 구조화 로그로 남기는 것이 지금 단계의 "피드백 루프" 구현이다
 * (EnsembleFraudDecisionService 참고) — 추후 확정되는 사기/정상 라벨과 결합해 재학습 파이프라인에
 * 흘려보내는 정식 파이프라인(라벨 저장소, 배치 조인 등)은 이번 범위 밖이다(docs/ARCHITECTURE.md
 * TODO 목록 참고).
 */
public record FraudDecision(
        String accountId,
        Action action,
        double combinedScore,
        Double modelProbability,
        String modelSource,
        double ruleScore,
        List<String> triggeredRules,
        Instant decidedAt) {
}
