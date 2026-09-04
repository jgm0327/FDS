package com.fdsv2.modelclient;

/**
 * TorchServe로의 실제 HTTP 호출만 담당하는 좁은 인터페이스.
 *
 * {@link TorchServeModelInferenceClient}에서 이 부분만 떼어낸 이유: Circuit Breaker/폴백 분기
 * 로직을 단위 테스트할 때 진짜 HTTP 서버 없이 "성공 응답"/"예외" 두 시나리오만 목으로 흉내내면
 * 충분해서다 — RestClient의 체이닝 API를 통째로 모킹하는 것보다 훨씬 간단하다.
 */
public interface TorchServeHttpCaller {

    /**
     * @param requestBodyJson TorchServePredictionRequest를 직렬화한 JSON 문자열
     * @return TorchServePredictionResponse를 담은 JSON 문자열
     * @throws RuntimeException 타임아웃/연결 실패/TorchServe가 에러 상태코드를 반환한 경우 등
     */
    String call(String requestBodyJson);
}
