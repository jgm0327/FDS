package com.fdsv2.modelclient;

/**
 * CP4(ai/pytorch-sequence-model) TorchServe 핸들러가 기대하는 거래 1건의 필드 형태.
 * (serving/handler.py docstring 참고 — 필드명이 {@link RawFeatureStep}과 다르다: gapSec vs
 * lastTxGapSec. gapSec은 null을 허용하지 않으므로 첫 거래는 0.0으로 채운다.)
 */
public record TorchServeTransactionStep(
        double amountRatio,
        double gapSec,
        boolean countryChanged,
        String merchantCategory) {

    static TorchServeTransactionStep from(RawFeatureStep step) {
        return new TorchServeTransactionStep(
                step.amountRatio(),
                step.lastTxGapSec() == null ? 0.0 : step.lastTxGapSec(),
                step.countryChanged(),
                step.merchantCategory());
    }
}
