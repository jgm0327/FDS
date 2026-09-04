package com.fdsv2.modelclient;

/**
 * {@link ModelInferenceClient}의 판정 결과.
 *
 * source는 CP4 성능 측정(docs/PERFORMANCE_MEASUREMENT.md "타임아웃/서킷브레이커 오픈 발생률")에서
 * "폴백이 얼마나 자주 발생하는지"를 나중에 관측하려 할 때, 응답 자체에 이미 근거가 남아있게
 * 하려는 의도다 — 정식 Prometheus 카운터 연동은 이번 범위 밖(다음 세션 TODO).
 */
public record FraudScore(String accountId, double fraudProbability, String source) {

    public static final String SOURCE_MODEL = "MODEL";
    public static final String SOURCE_FALLBACK = "FALLBACK";
}
