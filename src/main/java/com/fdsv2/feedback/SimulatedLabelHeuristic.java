package com.fdsv2.feedback;

import com.fdsv2.modelclient.RawFeatureStep;
import java.util.List;
import java.util.Random;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * CP6 — 실제 은행 라벨(사기 확정/정상 확인) 없이, 지연 도착하는 "진짜" 라벨을 시뮬레이션한다.
 *
 * <p><b>가정을 명시한다</b> — {@code ai/pytorch_sequence_model/tune_ensemble.py}의
 * {@code CostWeights}가 "실제 비용 데이터가 없어 가정임을 명시"해온 것과 같은 방식으로, 이
 * 휴리스틱도 명백히 가정이다: 판정 액션/모델 확률/규칙 점수는 절대 참조하지 않는다 — 참조하면
 * "모델이 예측한 걸 그대로 라벨로 되먹임"하는 순환 오류가 되어 재학습 자체가 무의미해진다. 대신
 * {@code ai/pytorch_sequence_model/data/synthetic.py}가 정의한 3가지 이상 패턴(금액 급증, 짧은
 * 간격의 연속 거래, 국가변경+금액증가)을 시퀀스 자체에서 그대로 재현해 "진짜" 이상 여부를 판단하고,
 * 여기에 대칭적 flip 노이즈(조사관도 완벽하지 않다는 가정)를 더한다.
 *
 * <p>synthetic.py와 값을 맞춘 이유: 나중에 이 시뮬레이션 라벨로 재학습한 모델을
 * {@code evaluate.py}/{@code tune_ensemble.py}가 이미 쓰는 합성 데이터와 나란히 비교할 수 있으려면,
 * "이상 패턴이 무엇인가"에 대한 정의 자체가 같아야 한다.
 */
@Component
public class SimulatedLabelHeuristic {

    private static final double AMOUNT_SPIKE_RATIO = 3.0;
    private static final long BURST_GAP_SECONDS = 30;
    private static final double COUNTRY_CHANGE_SPIKE_RATIO = 1.5;

    private final double flipProbability;
    private final Random random;

    public SimulatedLabelHeuristic(
            @Value("${fds.feedback.label-simulator.flip-probability:0.05}") double flipProbability,
            Random random) {
        this.flipProbability = flipProbability;
        this.random = random;
    }

    /**
     * @param sequence 판정에 실제로 쓰인, 클램프 이전의 원본 시퀀스({@link RawFeatureStep} —
     *                 {@code lastTxGapSec}이 null이면 "그 계좌의 첫 거래"라는 뜻이라, burst-frequency
     *                 판정에서 제외해야 한다. {@link FeedbackTransactionStep}으로 변환하면 이 null이
     *                 0.0으로 클램프돼 첫 거래를 전부 "burst"로 오판하게 되므로, 반드시 변환 전
     *                 원본으로 판단한다).
     */
    public int label(List<RawFeatureStep> sequence) {
        boolean anomalous = sequence.stream().anyMatch(this::isAnomalousStep);
        boolean flip = random.nextDouble() < flipProbability;
        boolean trueLabel = flip ? !anomalous : anomalous;
        return trueLabel ? 1 : 0;
    }

    private boolean isAnomalousStep(RawFeatureStep step) {
        boolean amountSpike = step.amountRatio() >= AMOUNT_SPIKE_RATIO;
        Long gap = step.lastTxGapSec();
        boolean burstFrequency = gap != null && gap >= 0 && gap <= BURST_GAP_SECONDS;
        boolean countryChangePlusSpike = step.countryChanged() && step.amountRatio() >= COUNTRY_CHANGE_SPIKE_RATIO;
        return amountSpike || burstFrequency || countryChangePlusSpike;
    }
}
