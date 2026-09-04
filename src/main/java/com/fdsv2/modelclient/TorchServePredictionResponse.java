package com.fdsv2.modelclient;

/** TorchServe REST 응답 바디 (serving/handler.py postprocess()가 내는 형태 그대로). */
public record TorchServePredictionResponse(String accountId, double fraudProbability) {
}
