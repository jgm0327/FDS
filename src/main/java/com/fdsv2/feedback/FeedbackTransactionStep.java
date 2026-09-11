package com.fdsv2.feedback;

import com.fdsv2.modelclient.RawFeatureStep;

/**
 * CP6(재학습 피드백 루프) 데이터셋에 담기는 거래 1건의 표현 — TorchServe 서빙 wire 스키마
 * (ai/pytorch_sequence_model/serving/handler.py docstring: amountRatio/gapSec/countryChanged/
 * merchantCategory)와 필드를 그대로 맞춘다. AI 쪽이 이 JSONL을 파싱할 때 필드 변환 없이 기존
 * {@code TransactionStep}/{@code AccountSequence} 스키마(ai/pytorch_sequence_model/data/schema.py)에
 * 바로 맞출 수 있게 하기 위함이다.
 *
 * <p>CP4의 {@code com.fdsv2.modelclient.TorchServeTransactionStep}과 필드가 동일하지만, 그
 * 클래스는 package-private(modelclient 패키지 전용)이라 재사용할 수 없다 — 대신 CP3/CP4가 서로의
 * 도메인 클래스에 의존하지 않고 "JSON 형태"만 계약으로 삼아온 것과 같은 원칙
 * ({@link RawFeatureStep} javadoc 참고)을 여기서도 그대로 지킨다. gapSec 클램프 로직(null/음수 ->
 * 0.0)도 {@code TorchServeTransactionStep.from()}과 동일하게 유지한다 — 판정에 실제로 입력된 값과
 * 재학습 데이터셋의 값이 어긋나면 train-serving skew가 재발하기 때문이다.
 */
public record FeedbackTransactionStep(
        double amountRatio,
        double gapSec,
        boolean countryChanged,
        String merchantCategory) {

    public static FeedbackTransactionStep from(RawFeatureStep step) {
        Long gap = step.lastTxGapSec();
        double gapSec = (gap == null || gap < 0) ? 0.0 : gap;
        return new FeedbackTransactionStep(
                step.amountRatio(), gapSec, step.countryChanged(), step.merchantCategory());
    }
}
