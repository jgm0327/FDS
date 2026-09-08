package com.fdsv2.decision;

import com.fdsv2.modelclient.AccountRecentSequenceReader;
import com.fdsv2.modelclient.RawFeatureStep;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * CP5 수동/부하 검증용 엔드포인트 — CP3 {@code FeatureQueryController}, CP4
 * {@code FraudScoreController}와 같은 이유로 기본 비활성화한다: 인증 없이 계좌ID만 알면 판정
 * 결과를 조회할 수 있는 API가 프로덕션에 실수로 남으면 안 된다.
 *
 * 거래마다 자동 판정({@link FraudDecisionEventListener})이 이미 로그로 남으므로, 이 엔드포인트는
 * "지금 이 계좌를 다시 판정하면 어떤 액션이 나오는지" 온디맨드로 확인/측정하기 위한 용도다
 * (docs/PERFORMANCE_MEASUREMENT.md CP5 "액션별 분포"/"end-to-end latency" k6 측정 진입점).
 */
@RestController
@RequiredArgsConstructor
@ConditionalOnProperty(
        prefix = "fds.decision",
        name = "query-endpoint-enabled",
        havingValue = "true",
        matchIfMissing = false)
public class FraudDecisionController {

    private final AccountRecentSequenceReader sequenceReader;
    private final FraudDecisionService fraudDecisionService;

    @GetMapping("/api/fraud-decision/{accountId}")
    public FraudDecision getFraudDecision(@PathVariable String accountId) {
        List<RawFeatureStep> steps = sequenceReader.readRecentSteps(accountId);
        RawFeatureStep latestStep = steps.isEmpty() ? null : steps.get(steps.size() - 1);
        return fraudDecisionService.decide(accountId, latestStep);
    }
}
