package com.fdsv2.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.fdsv2.modelclient.RawFeatureStep;
import com.fdsv2.modelclient.RuleBasedFallbackScorer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RuleEngineTest {

    @Mock
    private RuleBasedFallbackScorer fallbackScorer;

    private RuleEngine ruleEngine(String whitelistCsv, String blacklistCsv, double hardBlockThreshold) {
        return new RuleEngine(fallbackScorer, whitelistCsv, blacklistCsv, hardBlockThreshold);
    }

    @Test
    void 화이트리스트_계좌는_신호가_없어도_즉시_허용된다() {
        RuleEngine engine = ruleEngine("acc-safe,acc-vip", "", 20.0);

        RuleVerdict verdict = engine.evaluate("acc-safe", null);

        assertThat(verdict.override()).isEqualTo(Action.ALLOW);
        assertThat(verdict.triggeredRules()).containsExactly("whitelist");
    }

    @Test
    void 블랙리스트_계좌는_모델과_무관하게_즉시_차단된다() {
        RuleEngine engine = ruleEngine("", "acc-bad", 20.0);
        RawFeatureStep step = new RawFeatureStep("acc-bad", 1, 1.0, 10L, false, "GROCERY");

        RuleVerdict verdict = engine.evaluate("acc-bad", step);

        assertThat(verdict.override()).isEqualTo(Action.BLOCK);
        assertThat(verdict.triggeredRules()).containsExactly("blacklist");
    }

    @Test
    void 블랙리스트가_아니어도_금액배율_급증과_국가변경이_겹치면_하드룰로_즉시_차단된다() {
        RuleEngine engine = ruleEngine("", "", 20.0);
        RawFeatureStep step = new RawFeatureStep("acc-1", 3, 25.0, 5L, true, "CASH_ADVANCE");

        RuleVerdict verdict = engine.evaluate("acc-1", step);

        assertThat(verdict.override()).isEqualTo(Action.BLOCK);
        assertThat(verdict.triggeredRules()).containsExactly("extreme-amount-ratio-with-country-change");
    }

    @Test
    void 금액배율이_임계값_미만이면_국가가_바뀌어도_하드룰이_걸리지_않는다() {
        RuleEngine engine = ruleEngine("", "", 20.0);
        RawFeatureStep step = new RawFeatureStep("acc-1", 3, 6.0, 5L, true, "CASH_ADVANCE");
        when(fallbackScorer.score(step)).thenReturn(0.6);

        RuleVerdict verdict = engine.evaluate("acc-1", step);

        assertThat(verdict.override()).isNull();
        assertThat(verdict.ruleScore()).isEqualTo(0.6);
    }

    @Test
    void 블랙리스트와_하드룰이_동시에_걸리면_둘_다_triggeredRules에_OR로_남는다() {
        RuleEngine engine = ruleEngine("", "acc-bad", 20.0);
        RawFeatureStep step = new RawFeatureStep("acc-bad", 3, 25.0, 5L, true, "CASH_ADVANCE");

        RuleVerdict verdict = engine.evaluate("acc-bad", step);

        assertThat(verdict.override()).isEqualTo(Action.BLOCK);
        assertThat(verdict.triggeredRules())
                .containsExactlyInAnyOrder("blacklist", "extreme-amount-ratio-with-country-change");
    }

    @Test
    void 화이트리스트도_하드룰도_아니면_규칙기반스코어러_점수를_그대로_앙상블_입력으로_넘긴다() {
        RuleEngine engine = ruleEngine("", "", 20.0);
        RawFeatureStep step = new RawFeatureStep("acc-1", 1, 1.1, 100L, false, "GROCERY");
        when(fallbackScorer.score(step)).thenReturn(0.1);

        RuleVerdict verdict = engine.evaluate("acc-1", step);

        assertThat(verdict.override()).isNull();
        assertThat(verdict.ruleScore()).isEqualTo(0.1);
        assertThat(verdict.triggeredRules()).isEmpty();
    }

    @Test
    void 신호가_없어도_블랙리스트면_하드룰_금액조건_없이_차단된다() {
        RuleEngine engine = ruleEngine("", "acc-bad", 20.0);

        RuleVerdict verdict = engine.evaluate("acc-bad", null);

        assertThat(verdict.override()).isEqualTo(Action.BLOCK);
    }
}
