package com.fdsv2.decision;

import com.fdsv2.modelclient.FraudScore;
import com.fdsv2.modelclient.ModelInferenceClient;
import com.fdsv2.modelclient.RawFeatureStep;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
 *
 * <p>(backend/decision-ensemble-observability) {@code fds.decision.action.count}(태그 action)는
 * docs/PERFORMANCE_MEASUREMENT.md CP5 "액션별 분포" 판단 근거 — 임계값이 너무 엄격/느슨한지
 * 확인하려면 실제 트래픽에서 세 액션이 어떤 비율로 나오는지부터 봐야 한다.
 * {@code fds.decision.ensemble.combine.latency}는 같은 표 "앙상블 결합 연산 latency" —
 * {@link ModelInferenceClient#predict}(HTTP 왕복 포함, CP4 대시보드에서 이미 측정 중)는 제외하고
 * 가중합+임계값 비교라는 순수 연산만 감싼다. "보통 매우 짧아야 정상"이라는 표의 문구를 그대로
 * 검증하려는 목적이라, 여기에 predict() 호출까지 포함시키면 이 지표가 무의미해진다.
 */
@Slf4j
@Component
public class EnsembleFraudDecisionService implements FraudDecisionService {

    private final RuleEngine ruleEngine;
    private final ModelInferenceClient modelInferenceClient;
    private final MeterRegistry meterRegistry;
    private final double modelWeight;
    private final double ruleWeight;
    private final double lowThreshold;
    private final double highThreshold;

    public EnsembleFraudDecisionService(
            RuleEngine ruleEngine,
            ModelInferenceClient modelInferenceClient,
            MeterRegistry meterRegistry,
            @Value("${fds.decision.ensemble.model-weight:0.7}") double modelWeight,
            @Value("${fds.decision.ensemble.rule-weight:0.3}") double ruleWeight,
            @Value("${fds.decision.ensemble.low-threshold:0.3}") double lowThreshold,
            @Value("${fds.decision.ensemble.high-threshold:0.7}") double highThreshold) {
        this.ruleEngine = ruleEngine;
        this.modelInferenceClient = modelInferenceClient;
        this.meterRegistry = meterRegistry;
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
        // 남긴다. 라벨 결합/재학습 파이프라인 연동은 이번 범위 밖(docs/ARCHITECTURE.md TODO 목록).
        log.info("CP5 판정 결과: {}", decision);
        meterRegistry.counter("fds.decision.action.count", "action", decision.action().name()).increment();
        return decision;
    }

    private FraudDecision immediateDecision(String accountId, RuleVerdict verdict) {
        return new FraudDecision(
                accountId, verdict.override(), verdict.ruleScore(), null, null,
                verdict.ruleScore(), verdict.triggeredRules(), Instant.now());
    }

    private FraudDecision ensembleDecision(String accountId, RuleVerdict verdict) {
        FraudScore modelScore = modelInferenceClient.predict(accountId);
        return Timer.builder("fds.decision.ensemble.combine.latency")
                .description("docs/PERFORMANCE_MEASUREMENT.md CP5 - 앙상블 결합 연산 latency"
                        + " (모델 HTTP 호출 제외, 가중합+임계값 비교만)")
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(() -> {
                    double combinedScore =
                            modelWeight * modelScore.fraudProbability() + ruleWeight * verdict.ruleScore();
                    Action action = toAction(combinedScore);
                    return new FraudDecision(
                            accountId, action, combinedScore, modelScore.fraudProbability(), modelScore.source(),
                            verdict.ruleScore(), verdict.triggeredRules(), Instant.now());
                });
    }

    private Action toAction(double combinedScore) {
        // 코드 리뷰 지적: combinedScore가 NaN이면(예: TorchServe가 이상값을 내려보낸 경우) 아래
        // "<" 비교가 전부 false로 평가되어 조용히 BLOCK으로 떨어진다 — 이 값이 "고위험으로 확인된
        // BLOCK"인지 "판단 불가"인지 구분이 안 된다. 판단 불가 상태를 확정적 차단(BLOCK)보다는
        // 추가인증(STEP_UP_AUTH)으로 완충하는 게 3단계 액션의 취지("애매한 구간 완충")에 더 맞는다.
        if (Double.isNaN(combinedScore)) {
            log.warn("CP5 combinedScore가 NaN — 모델 응답 이상 의심, STEP_UP_AUTH로 완충 처리");
            return Action.STEP_UP_AUTH;
        }
        if (combinedScore < lowThreshold) {
            return Action.ALLOW;
        }
        if (combinedScore < highThreshold) {
            return Action.STEP_UP_AUTH;
        }
        return Action.BLOCK;
    }
}
