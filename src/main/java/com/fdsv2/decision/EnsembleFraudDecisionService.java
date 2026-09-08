package com.fdsv2.decision;

import com.fdsv2.modelclient.FraudScore;
import com.fdsv2.modelclient.ModelInferenceClient;
import com.fdsv2.modelclient.RawFeatureStep;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * CP5 판정 엔진의 실제 구현 (docs/ARCHITECTURE.md 5번).
 *
 * {@link RuleEngine}이 화이트리스트/하드룰로 즉시 결론을 낸 경우 모델 호출을 건너뛴다("모델 확률과
 * 무관하게 즉시 반영") — 애매한 구간만 {@link ModelInferenceClient}를 호출해 모델 확률과 규칙
 * 점수를 가중합한다.
 *
 * <p>combinedScore = modelWeight * modelProbability + ruleWeight * ruleScore. 모델이 시퀀스 전체를
 * 보고 학습된 신호라, 규칙 기반 점수({@code RuleBasedFallbackScorer}, 4단계 이산값)보다 기본
 * 가중치를 더 준다(0.7 : 0.3) — ARCHITECTURE.md 5번 "애매한 구간은 가중합으로 세분화".
 * combinedScore를 lowThreshold/highThreshold와 비교해 ALLOW/STEP_UP_AUTH/BLOCK 3단계로 나눈다.
 */
@Slf4j
@Component
public class EnsembleFraudDecisionService implements FraudDecisionService {

    private final RuleEngine ruleEngine;
    private final ModelInferenceClient modelInferenceClient;
    private final double modelWeight;
    private final double ruleWeight;
    private final double lowThreshold;
    private final double highThreshold;

    public EnsembleFraudDecisionService(
            RuleEngine ruleEngine,
            ModelInferenceClient modelInferenceClient,
            @Value("${fds.decision.ensemble.model-weight:0.7}") double modelWeight,
            @Value("${fds.decision.ensemble.rule-weight:0.3}") double ruleWeight,
            @Value("${fds.decision.ensemble.low-threshold:0.3}") double lowThreshold,
            @Value("${fds.decision.ensemble.high-threshold:0.7}") double highThreshold) {
        this.ruleEngine = ruleEngine;
        this.modelInferenceClient = modelInferenceClient;
        this.modelWeight = modelWeight;
        this.ruleWeight = ruleWeight;
        this.lowThreshold = lowThreshold;
        this.highThreshold = highThreshold;
    }

    @Override
    public FraudDecision decide(String accountId, RawFeatureStep latestStep) {
        RuleVerdict verdict = ruleEngine.evaluate(accountId, latestStep);

        FraudDecision decision = verdict.override() != null
                ? immediateDecision(accountId, verdict)
                : ensembleDecision(accountId, verdict);

        // CP5 피드백 루프(docs/ARCHITECTURE.md 5번)의 현재 범위 — 구조화 로그 한 줄로 판정 결과를
        // 남긴다. 라벨 결합/재학습 파이프라인 연동, Prometheus 액션 분포 카운터는 이번 범위 밖
        // (다음 세션 TODO, 다른 CP들처럼 별도 -observability 브랜치로).
        log.info("CP5 판정 결과: {}", decision);
        return decision;
    }

    private FraudDecision immediateDecision(String accountId, RuleVerdict verdict) {
        return new FraudDecision(
                accountId, verdict.override(), verdict.ruleScore(), null, null,
                verdict.ruleScore(), verdict.triggeredRules(), Instant.now());
    }

    private FraudDecision ensembleDecision(String accountId, RuleVerdict verdict) {
        FraudScore modelScore = modelInferenceClient.predict(accountId);
        double combinedScore = modelWeight * modelScore.fraudProbability() + ruleWeight * verdict.ruleScore();
        Action action = toAction(combinedScore);
        return new FraudDecision(
                accountId, action, combinedScore, modelScore.fraudProbability(), modelScore.source(),
                verdict.ruleScore(), verdict.triggeredRules(), Instant.now());
    }

    private Action toAction(double combinedScore) {
        if (combinedScore < lowThreshold) {
            return Action.ALLOW;
        }
        if (combinedScore < highThreshold) {
            return Action.STEP_UP_AUTH;
        }
        return Action.BLOCK;
    }
}
