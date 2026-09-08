package com.fdsv2.modelclient;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * {@link TorchServeTransactionStep#from(RawFeatureStep)}의 gapSec 방어 로직 검증 —
 * 실제로 k6 동시 부하 재현 중 음수 gapSec이 모델에 NaN을 유발한 문제(클래스 javadoc 참고)를
 * 회귀 방지하기 위한 테스트.
 */
class TorchServeTransactionStepTest {

    @Test
    void gapSec이_null이면_0으로_채운다() {
        RawFeatureStep step = new RawFeatureStep("acc-1", 1, 1.0, null, false, "GROCERY");

        TorchServeTransactionStep result = TorchServeTransactionStep.from(step);

        assertThat(result.gapSec()).isZero();
    }

    @Test
    void gapSec이_음수면_0으로_clamp한다() {
        // 실측 재현: 같은 계좌에 시간 역순으로 이벤트가 쌓이면(예: 워밍업 스크립트 재실행) 음수
        // gapSec이 만들어지고, 이걸 그대로 넘기면 모델의 log1p(gapSec) 정규화가 NaN을 낸다.
        RawFeatureStep step = new RawFeatureStep("acc-1", 2, 1.0, -5233L, false, "GROCERY");

        TorchServeTransactionStep result = TorchServeTransactionStep.from(step);

        assertThat(result.gapSec()).isZero();
    }

    @Test
    void gapSec이_양수면_그대로_전달한다() {
        RawFeatureStep step = new RawFeatureStep("acc-1", 2, 1.0, 1800L, false, "GROCERY");

        TorchServeTransactionStep result = TorchServeTransactionStep.from(step);

        assertThat(result.gapSec()).isEqualTo(1800.0);
    }
}
