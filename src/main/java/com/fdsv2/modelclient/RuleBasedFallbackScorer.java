package com.fdsv2.modelclient;

import org.springframework.stereotype.Component;

/**
 * TorchServe 호출 실패/타임아웃/서킷브레이커 오픈 시 사용하는 규칙 기반 기본 스코어
 * (docs/ARCHITECTURE.md 4번 "장애 대응").
 *
 * 이건 CP5(판정 및 대응 — 규칙 엔진 + 앙상블)의 정식 하드룰이 아니다. CP5가 아직 없는 지금
 * 시점에, 서킷브레이커 폴백 경로가 "그냥 실패"가 아니라 최소한 계좌의 최근 신호를 반영한
 * 스코어를 내도록 하는 임시 placeholder다. CP5가 만들어지면 이 클래스는 CP5의 규칙 엔진으로
 * 대체/흡수될 가능성이 높다.
 */
@Component
public class RuleBasedFallbackScorer {

    private static final double HIGH_AMOUNT_RATIO_THRESHOLD = 5.0;

    /**
     * @param latestStep 계좌의 가장 최근 거래 스텝(없으면 null — 신규/비활성 계좌 등 신호가 전혀 없는 경우)
     */
    public double score(RawFeatureStep latestStep) {
        if (latestStep == null) {
            // 신호 자체가 없으면 위험도를 판단할 근거가 없다 — "모르겠다"는 뜻의 중립값.
            return 0.5;
        }
        boolean highAmount = latestStep.amountRatio() >= HIGH_AMOUNT_RATIO_THRESHOLD;
        boolean countryChanged = latestStep.countryChanged();

        if (highAmount && countryChanged) {
            return 0.9;
        }
        if (highAmount || countryChanged) {
            return 0.6;
        }
        return 0.1;
    }
}
