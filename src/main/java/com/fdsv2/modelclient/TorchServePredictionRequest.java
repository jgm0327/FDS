package com.fdsv2.modelclient;

import java.util.List;

/** TorchServe REST 요청 바디 (serving/handler.py preprocess()가 기대하는 형태 그대로). */
public record TorchServePredictionRequest(String accountId, List<TorchServeTransactionStep> transactions) {
}
