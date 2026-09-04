package com.fdsv2.modelclient;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RuleBasedFallbackScorerTest {

    private final RuleBasedFallbackScorer scorer = new RuleBasedFallbackScorer();

    @Test
    void 신호가_없으면_중립값을_낸다() {
        assertThat(scorer.score(null)).isEqualTo(0.5);
    }

    @Test
    void 평범한_거래는_낮은_점수를_낸다() {
        RawFeatureStep step = new RawFeatureStep("acc-1", 3, 1.1, 100L, false, "GROCERY");

        assertThat(scorer.score(step)).isEqualTo(0.1);
    }

    @Test
    void 금액배율만_높으면_중간_점수를_낸다() {
        RawFeatureStep step = new RawFeatureStep("acc-1", 3, 6.0, 100L, false, "GROCERY");

        assertThat(scorer.score(step)).isEqualTo(0.6);
    }

    @Test
    void 국가변경만_있으면_중간_점수를_낸다() {
        RawFeatureStep step = new RawFeatureStep("acc-1", 3, 1.0, 100L, true, "GROCERY");

        assertThat(scorer.score(step)).isEqualTo(0.6);
    }

    @Test
    void 금액배율_급증과_국가변경이_겹치면_높은_점수를_낸다() {
        RawFeatureStep step = new RawFeatureStep("acc-1", 3, 7.0, 10L, true, "CASH_ADVANCE");

        assertThat(scorer.score(step)).isEqualTo(0.9);
    }
}
