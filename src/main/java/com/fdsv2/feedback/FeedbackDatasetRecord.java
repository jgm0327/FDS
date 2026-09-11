package com.fdsv2.feedback;

import com.fdsv2.decision.Action;
import java.time.Instant;
import java.util.List;

/**
 * CP6 — 재학습에 쓰이는 최종 조인 레코드 하나. {@link FeedbackLabelScheduler}가
 * {@link PendingDecisionRecord}의 라벨 공개 시각이 지나면 이 형태로 변환해
 * {@link FeedbackDatasetWriter}를 통해 JSONL 한 줄로 남긴다.
 *
 * <p>{@code transactions} 필드는 TorchServe 서빙 wire 스키마와 필드를 맞춘
 * {@link FeedbackTransactionStep} 그대로다({@link FeedbackTransactionStep} javadoc 참고) — AI 쪽이
 * 이 JSONL을 파싱해 {@code AccountSequence}/{@code TransactionStep}으로 바로 맞출 수 있게 하기
 * 위함이다.
 */
public record FeedbackDatasetRecord(
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
        Instant labeledAt) {
}
