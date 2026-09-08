package com.fdsv2.decision;

import com.fdsv2.modelclient.RawFeatureStep;
import com.fdsv2.modelclient.RuleBasedFallbackScorer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * CP5 규칙 엔진 (docs/ARCHITECTURE.md 5번) — 화이트리스트 + 하드룰(OR)을 평가한다.
 *
 * <p>화이트리스트/하드룰 어느 쪽에도 안 걸리는 애매한 구간의 "규칙 점수"는 새로 만들지 않고
 * {@link RuleBasedFallbackScorer}(CP4, 서킷브레이커 폴백용으로 이미 존재)를 그대로 재사용한다.
 * 그 클래스의 javadoc이 "CP5가 만들어지면 이 클래스가 CP5의 규칙 엔진으로 흡수될 가능성이 높다"고
 * 이미 예고했었다 — 다만 그 폴백 스코어러 주변 배선(CircuitBreaker 등, ModelClientConfig /
 * TorchServeModelInferenceClient)은 병렬로 진행 중인 backend/model-client-concurrency-fix
 * 브랜치와 겹칠 위험이 있어, 이번엔 흡수(클래스 삭제/병합)까지는 하지 않고 "읽기 전용으로 재사용"만
 * 한다 — score() 계산 로직 자체나 그 주변 파일을 전혀 건드리지 않으므로 그 브랜치와 충돌할 여지가
 * 없다.
 *
 * <p>화이트리스트를 하드룰보다 먼저 평가한다 — 화이트리스트는 "이 계좌는 이미 신뢰한다"는 운영자의
 * 명시적 의사결정이므로, 하드룰(자동 탐지 휴리스틱)보다 우선한다.
 */
@Component
public class RuleEngine {

    private final RuleBasedFallbackScorer fallbackScorer;
    private final Set<String> whitelistedAccountIds;
    private final Set<String> blacklistedAccountIds;
    private final double hardBlockAmountRatioThreshold;

    public RuleEngine(
            RuleBasedFallbackScorer fallbackScorer,
            @Value("${fds.decision.whitelisted-account-ids:}") String whitelistedAccountIdsCsv,
            @Value("${fds.decision.blacklisted-account-ids:}") String blacklistedAccountIdsCsv,
            @Value("${fds.decision.hard-block-amount-ratio-threshold:20.0}") double hardBlockAmountRatioThreshold) {
        this.fallbackScorer = fallbackScorer;
        this.whitelistedAccountIds = parseCsv(whitelistedAccountIdsCsv);
        this.blacklistedAccountIds = parseCsv(blacklistedAccountIdsCsv);
        this.hardBlockAmountRatioThreshold = hardBlockAmountRatioThreshold;
    }

    /**
     * @param latestStep 계좌의 가장 최근 거래 스텝(없으면 null — 신규/비활성 계좌 등)
     */
    public RuleVerdict evaluate(String accountId, RawFeatureStep latestStep) {
        if (whitelistedAccountIds.contains(accountId)) {
            return new RuleVerdict(Action.ALLOW, 0.0, List.of("whitelist"));
        }

        // 하드룰 — OR 조건으로 즉시 반영(docs/ARCHITECTURE.md 5번). 하나라도 걸리면 나머지는
        // 평가할 필요 없이 즉시 차단이지만, 어떤 룰이 걸렸는지는 전부 기록해서 판정 로그에 남긴다.
        List<String> triggeredHardRules = new ArrayList<>();
        if (blacklistedAccountIds.contains(accountId)) {
            triggeredHardRules.add("blacklist");
        }
        if (latestStep != null
                && latestStep.amountRatio() >= hardBlockAmountRatioThreshold
                && latestStep.countryChanged()) {
            triggeredHardRules.add("extreme-amount-ratio-with-country-change");
        }
        if (!triggeredHardRules.isEmpty()) {
            return new RuleVerdict(Action.BLOCK, 1.0, triggeredHardRules);
        }

        double ruleScore = fallbackScorer.score(latestStep);
        return new RuleVerdict(null, ruleScore, List.of());
    }

    private static Set<String> parseCsv(String csv) {
        if (csv == null || csv.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }
}
