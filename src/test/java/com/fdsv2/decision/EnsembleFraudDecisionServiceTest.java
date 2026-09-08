package com.fdsv2.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fdsv2.modelclient.FraudScore;
import com.fdsv2.modelclient.ModelInferenceClient;
import com.fdsv2.modelclient.RawFeatureStep;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EnsembleFraudDecisionServiceTest {

    @Mock
    private RuleEngine ruleEngine;

    @Mock
    private ModelInferenceClient modelInferenceClient;

    private EnsembleFraudDecisionService service(double modelWeight, double ruleWeight, double low, double high) {
        return new EnsembleFraudDecisionService(ruleEngine, modelInferenceClient, modelWeight, ruleWeight, low, high);
    }

    @Test
    void 규칙엔진이_즉시_액션을_정하면_모델을_호출하지_않는다() {
        EnsembleFraudDecisionService service = service(0.7, 0.3, 0.3, 0.7);
        RawFeatureStep step = new RawFeatureStep("acc-bad", 1, 1.0, 10L, false, "GROCERY");
        when(ruleEngine.evaluate("acc-bad", step)).thenReturn(new RuleVerdict(Action.BLOCK, 1.0, List.of("blacklist")));

        FraudDecision decision = service.decide("acc-bad", step);

        assertThat(decision.action()).isEqualTo(Action.BLOCK);
        assertThat(decision.combinedScore()).isEqualTo(1.0);
        assertThat(decision.modelProbability()).isNull();
        assertThat(decision.modelSource()).isNull();
        assertThat(decision.triggeredRules()).containsExactly("blacklist");
        verify(modelInferenceClient, never()).predict("acc-bad");
    }

    @Test
    void 애매한_구간이면_모델확률과_규칙점수를_가중합해서_액션을_정한다() {
        EnsembleFraudDecisionService service = service(0.7, 0.3, 0.3, 0.7);
        RawFeatureStep step = new RawFeatureStep("acc-1", 3, 1.1, 100L, false, "GROCERY");
        when(ruleEngine.evaluate("acc-1", step)).thenReturn(new RuleVerdict(null, 0.1, List.of()));
        when(modelInferenceClient.predict("acc-1")).thenReturn(new FraudScore("acc-1", 0.5, FraudScore.SOURCE_MODEL));

        FraudDecision decision = service.decide("acc-1", step);

        // combinedScore = 0.7*0.5 + 0.3*0.1 = 0.38
        assertThat(decision.combinedScore()).isCloseTo(0.38, within(1e-9));
        assertThat(decision.action()).isEqualTo(Action.STEP_UP_AUTH);
        assertThat(decision.modelProbability()).isEqualTo(0.5);
        assertThat(decision.modelSource()).isEqualTo(FraudScore.SOURCE_MODEL);
        assertThat(decision.ruleScore()).isEqualTo(0.1);
    }

    @Test
    void combinedScore가_lowThreshold_미만이면_허용한다() {
        EnsembleFraudDecisionService service = service(0.7, 0.3, 0.3, 0.7);
        RawFeatureStep step = new RawFeatureStep("acc-1", 1, 1.0, null, false, "GROCERY");
        when(ruleEngine.evaluate("acc-1", step)).thenReturn(new RuleVerdict(null, 0.0, List.of()));
        when(modelInferenceClient.predict("acc-1")).thenReturn(new FraudScore("acc-1", 0.1, FraudScore.SOURCE_MODEL));

        FraudDecision decision = service.decide("acc-1", step);

        assertThat(decision.action()).isEqualTo(Action.ALLOW);
    }

    @Test
    void combinedScore가_highThreshold_이상이면_차단한다() {
        EnsembleFraudDecisionService service = service(0.7, 0.3, 0.3, 0.7);
        RawFeatureStep step = new RawFeatureStep("acc-1", 1, 1.0, null, false, "GROCERY");
        when(ruleEngine.evaluate("acc-1", step)).thenReturn(new RuleVerdict(null, 0.9, List.of()));
        when(modelInferenceClient.predict("acc-1")).thenReturn(new FraudScore("acc-1", 0.95, FraudScore.SOURCE_MODEL));

        FraudDecision decision = service.decide("acc-1", step);

        assertThat(decision.action()).isEqualTo(Action.BLOCK);
    }

    @Test
    void 서킷브레이커_폴백으로_나온_모델점수도_그대로_앙상블에_반영된다() {
        // ModelInferenceClient는 절대 예외를 던지지 않고, 실패 시 FALLBACK source로 스코어를 낸다
        // (해당 클래스 계약) — 여기서는 그 반환값을 있는 그대로 신뢰하고 앙상블에 반영하는지만 본다.
        EnsembleFraudDecisionService service = service(0.7, 0.3, 0.3, 0.7);
        RawFeatureStep step = new RawFeatureStep("acc-1", 1, 1.0, null, false, "GROCERY");
        when(ruleEngine.evaluate("acc-1", step)).thenReturn(new RuleVerdict(null, 0.1, List.of()));
        when(modelInferenceClient.predict("acc-1")).thenReturn(new FraudScore("acc-1", 0.5, FraudScore.SOURCE_FALLBACK));

        FraudDecision decision = service.decide("acc-1", step);

        assertThat(decision.modelSource()).isEqualTo(FraudScore.SOURCE_FALLBACK);
    }
}
