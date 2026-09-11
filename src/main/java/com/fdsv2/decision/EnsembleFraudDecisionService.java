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
 * <p>combinedScore = modelWeight * modelProbability + ruleWeight * ruleScore.
 * combinedScore를 lowThreshold/highThreshold와 비교해 ALLOW/STEP_UP_AUTH/BLOCK 3단계로 나눈다.
 *
 * <p><b>기본값의 근거(backend/ai/ensemble-weight-tuning)</b>: 이 네 값(0.7:0.3, 0.3/0.7)은 원래
 * "모델이 시퀀스 전체를 보니 규칙보다 믿을 만하다"는 정성적 직관으로 하드코딩돼 있었다. 이후
 * {@code ai/pytorch_sequence_model/tune_ensemble.py}가 합성 라벨 데이터(학습에 쓴 것과 같은
 * seed/분포)로 그리드서치를 돌려 실제로 검증했고, 결과는 modelWeight=1.0/ruleWeight=0.0,
 * lowThreshold=0.20/highThreshold=0.75 — 애매한 구간에서는 규칙점수를 아예 섞지 않는 쪽이
 * 기대 비용을 더 낮췄다(test set 기준 하드코딩값 대비 약 28% 낮음, 세부 수치는
 * docs/sessions/2026-09-11_ai-ensemble-weight-tuning_session-01.md 참고). ruleWeight=0이어도
 * 규칙 신호가 완전히 사라지는 건 아니다 — {@link ModelInferenceClient}가 서킷브레이커
 * 오픈/타임아웃 시 반환하는 폴백 확률 자체가 {@code RuleBasedFallbackScorer}의 출력이라(
 * {@code FraudScore.SOURCE_FALLBACK}), modelProbability 자리에 규칙 기반 값이 자동으로
 * 대입되는 구조다. 다만 이 결과는 합성 데이터와 이 스크립트가 가정한 비용(사기 완전누출이
 * 가장 나쁘다는 등) 위에서 나온 것이라, 실제 라벨 데이터가 쌓이면 재검증이 필요하다.
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
