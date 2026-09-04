package com.fdsv2.modelclient;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * CP4 수동/부하 검증용 엔드포인트 — CP3의 {@code FeatureQueryController}와 같은 이유로 기본
 * 비활성화한다: 인증 없이 계좌ID만 알면 사기 확률을 조회할 수 있는 API가 CP5(판정 및 대응)가
 * 나오기 전까지 프로덕션에 실수로 남아있으면 안 된다. k6로 CP4 지표를 측정할 때만
 * fds.model-serving.query-endpoint-enabled=true로 켠다.
 *
 * CP5가 만들어지면 이 엔드포인트는 "판정 및 대응" 흐름(규칙 엔진 + 앙상블)으로 흡수될 예정이라,
 * 그 전까지의 임시 진입점이다.
 */
@RestController
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "fds.model-serving",
        name = "query-endpoint-enabled",
        havingValue = "true",
        matchIfMissing = false)
public class FraudScoreController {

    private final ModelInferenceClient modelInferenceClient;

    @GetMapping("/api/fraud-score/{accountId}")
    public FraudScore getFraudScore(@PathVariable String accountId) {
        return modelInferenceClient.predict(accountId);
    }
}
