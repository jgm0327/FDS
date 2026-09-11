package com.fdsv2.feedback;

import com.fdsv2.decision.Action;
import java.time.Instant;
import java.util.List;

/**
 * CP6 — 라벨이 "공개"(reveal)될 시각까지 Redis({@code feedback:pending:{decisionId}})에 저장해두는
 * 판정 스냅샷.
 *
 * <p><b>라벨은 기록 시점에 이미 계산돼 있다</b> — {@link FeedbackDecisionRecorder}가
 * {@code SimulatedLabelHeuristic}으로 라벨을 즉시 계산해서 이 레코드에 담아둔다. 실제 은행
 * 업무도 "사기 여부 자체는 그 거래가 일어난 순간 이미 정해져 있고, 그걸 확인(신고/조사/이의제기
 * 처리)하는 데 시간이 걸릴 뿐"이라는 점을 그대로 반영한 설계다 — 그래서 지연되는 건 라벨의 "계산"이
 * 아니라 "공개"뿐이고, {@link FeedbackLabelScheduler}는 만기된 레코드를 다시 채점하지 않고 이미
 * 계산된 라벨을 데이터셋으로 옮기기만 한다.
 */
public record PendingDecisionRecord(
        String decisionId,
        String accountId,
        Instant decidedAt,
        Action action,
        double combinedScore,
        Double modelProbability,
        String modelSource,
        double ruleScore,
        List<String> triggeredRules,
        List<FeedbackTransactionStep> transactions,
        int label,
        String labelSource,
        long labelDelaySeconds,
        Instant scheduledRevealAt) {
}
