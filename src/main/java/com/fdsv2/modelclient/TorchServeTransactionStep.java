package com.fdsv2.modelclient;

/**
 * CP4(ai/pytorch-sequence-model) TorchServe 핸들러가 기대하는 거래 1건의 필드 형태.
 * (serving/handler.py docstring 참고 — 필드명이 {@link RawFeatureStep}과 다르다: gapSec vs
 * lastTxGapSec. gapSec은 null을 허용하지 않으므로 첫 거래는 0.0으로 채운다.)
 *
 * <p><b>음수 gapSec도 0.0으로 clamp하는 이유</b> — (backend/model-client-concurrency-fix)
 * k6 동시 부하 재현 중 실제로 겪은 문제: 계좌의 최근 거래 시퀀스에 {@code lastTxGapSec}이 음수인
 * 항목이 섞여 있으면(예: k6 워밍업 스크립트를 같은 계좌에 여러 번 재실행해서 "지금 시각 -N분"
 * 오프셋으로 만든 합성 타임스탬프가 이미 저장된 이전 실행의 타임스탬프보다 앞서게 되는 경우 —
 * 즉 사건이 시간 역순으로 기록되는 경우), 모델 forward의 log1p(gapSec) 정규화(ai/README.md
 * "핵심 설계 결정" 3번)가 log(1+음수)를 계산하게 되어 NaN을 낸다. TorchServe는 이 NaN을 그대로
 * JSON 응답(fraudProbability: NaN)에 담아 반환하는데, 표준 JSON은 NaN 리터럴을 허용하지 않아
 * Jackson이 파싱에 실패하고 이게 그대로 Circuit Breaker의 "실패"로 잡힌다 — 정작 TorchServe도
 * 모델도 멀쩡히 응답했는데 응답 내용 자체가 모델이 학습한 입력 범위를 벗어난 값이라 생긴 문제다.
 * 실서비스에서도 재전송/시계 오차 등으로 이벤트가 시간 역순으로 도착할 가능성이 있으므로, 첫
 * 거래(gapSec 없음)와 같은 방식으로 방어적으로 0.0 처리한다. 근본 원인(같은 계좌에 시간 역순
 * 이벤트가 애초에 쌓이지 않게 하는 것)은 시퀀스 집계 쪽(backend/sequence-window-feature-store)
 * 책임이라 이번 범위 밖으로 남겨두고, 여기서는 모델 호출부가 입력 방어만 한다.</p>
 */
public record TorchServeTransactionStep(
        double amountRatio,
        double gapSec,
        boolean countryChanged,
        String merchantCategory) {

    static TorchServeTransactionStep from(RawFeatureStep step) {
        Long gap = step.lastTxGapSec();
        double gapSec = (gap == null || gap < 0) ? 0.0 : gap;
        return new TorchServeTransactionStep(
                step.amountRatio(),
                gapSec,
                step.countryChanged(),
                step.merchantCategory());
    }
}
