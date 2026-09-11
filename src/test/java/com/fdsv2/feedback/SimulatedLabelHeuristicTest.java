package com.fdsv2.feedback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.fdsv2.modelclient.RawFeatureStep;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SimulatedLabelHeuristicTest {

    @Mock
    private Random random;

    private SimulatedLabelHeuristic heuristic(double flipProbability) {
        return new SimulatedLabelHeuristic(flipProbability, random);
    }

    private RawFeatureStep step(double amountRatio, Long gapSec, boolean countryChanged) {
        return new RawFeatureStep("acc-1", 1, amountRatio, gapSec, countryChanged, "GROCERY");
    }

    @Test
    void 평범한_시퀀스는_노이즈가_없으면_정상으로_라벨링한다() {
        when(random.nextDouble()).thenReturn(1.0); // flipProbability보다 항상 크게 만들어 flip 없음
        List<RawFeatureStep> sequence = List.of(step(1.0, 300L, false), step(1.1, 250L, false));

        assertThat(heuristic(0.05).label(sequence)).isZero();
    }

    @Test
    void 금액이_3배_이상_급증하면_사기로_라벨링한다() {
        when(random.nextDouble()).thenReturn(1.0);
        List<RawFeatureStep> sequence = List.of(step(1.0, 300L, false), step(4.0, 200L, false));

        assertThat(heuristic(0.05).label(sequence)).isEqualTo(1);
    }

    @Test
    void 간격이_30초_이하로_급격히_짧아지면_사기로_라벨링한다() {
        when(random.nextDouble()).thenReturn(1.0);
        List<RawFeatureStep> sequence = List.of(step(1.0, 300L, false), step(1.0, 5L, false));

        assertThat(heuristic(0.05).label(sequence)).isEqualTo(1);
    }

    @Test
    void 국가변경과_1점5배_이상_금액증가가_겹치면_사기로_라벨링한다() {
        when(random.nextDouble()).thenReturn(1.0);
        List<RawFeatureStep> sequence = List.of(step(1.6, 300L, true));

        assertThat(heuristic(0.05).label(sequence)).isEqualTo(1);
    }

    @Test
    void 국가변경만_있고_금액증가가_기준_미만이면_정상으로_라벨링한다() {
        when(random.nextDouble()).thenReturn(1.0);
        List<RawFeatureStep> sequence = List.of(step(1.2, 300L, true));

        assertThat(heuristic(0.05).label(sequence)).isZero();
    }

    @Test
    void 첫_거래의_lastTxGapSec_null은_burst로_오판하지_않는다() {
        // 회귀 테스트: FeedbackTransactionStep으로 변환하면 null이 0.0으로 클램프되어 "간격 30초
        // 이하"에 걸려버린다 — 반드시 클램프 이전의 RawFeatureStep(null 유지)으로 판단해야 한다.
        when(random.nextDouble()).thenReturn(1.0);
        List<RawFeatureStep> sequence = List.of(step(1.0, null, false));

        assertThat(heuristic(0.05).label(sequence)).isZero();
    }

    @Test
    void flip_노이즈가_뽑히면_원래_라벨을_뒤집는다() {
        when(random.nextDouble()).thenReturn(0.0); // < flipProbability(0.05) -> flip
        List<RawFeatureStep> normalSequence = List.of(step(1.0, 300L, false));

        assertThat(heuristic(0.05).label(normalSequence)).isEqualTo(1);
    }
}
