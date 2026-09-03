package com.fdsv2.sequence;

import com.fdsv2.transaction.TransactionEvent;
import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Duration;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * CP2 핵심 로직 — 계좌ID(accountId)별로 State Store에 누적 상태를 유지하면서, 매 거래마다
 * AccountFeatureVector(슬라이딩 윈도우 통계)를 계산해서 내보낸다 (docs/ARCHITECTURE.md 2번 참고).
 *
 * DSL의 SlidingWindows/TimeWindows 대신 Processor API + 커스텀 State Store를 쓰는 이유:
 * 4개 지표(최근 건수/금액배율/경과시간/국가변경)가 서로 다른 계산이라 윈도우 집계 하나로 표현이
 * 안 되고, 계좌별 상태 하나에 다 같이 묶어서 계산하는 게 더 자연스럽다.
 *
 * accountId가 파티션 키이므로(CP1), 같은 계좌의 이벤트는 항상 같은 태스크로 들어와서 이 프로세서
 * 인스턴스가 순서대로 처리한다는 게 이 로직의 핵심 전제다. TransactionEvent.occurredAt()(이벤트 발생
 * 시각)을 기준으로 계산하며, 클라이언트 시계 오차 등으로 occurredAt이 역전되는 극단적인 경우는
 * 이번 범위에서 다루지 않는다 (다음 개선 후보).
 */
@Slf4j
public class AccountActivityProcessor implements FixedKeyProcessor<String, TransactionEvent, AccountFeatureVector> {

    private final String storeName;
    private final Duration recentWindow;

    private FixedKeyProcessorContext<String, AccountFeatureVector> context;
    private KeyValueStore<String, AccountActivityState> store;

    public AccountActivityProcessor(String storeName, Duration recentWindow) {
        this.storeName = storeName;
        this.recentWindow = recentWindow;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, AccountFeatureVector> context) {
        this.context = context;
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, TransactionEvent> record) {
        String accountId = record.key();
        TransactionEvent event = record.value();
        Instant eventTime = event.occurredAt();

        AccountActivityState state = store.get(accountId);
        if (state == null) {
            state = new AccountActivityState();
        }

        int recentCount = updateRecentWindowCount(state, eventTime);
        double amountRatio = calculateAmountRatio(state, event.amount());
        Long lastTxGapSec = calculateGapSeconds(state, eventTime);
        boolean countryChanged = state.getLastCountry() != null && !state.getLastCountry().equals(event.country());

        // 다음 거래를 위해 상태 갱신 (누적 합계/건수, 마지막 거래 시각/국가).
        state.setTotalAmount(state.getTotalAmount().add(event.amount()));
        state.setTotalCount(state.getTotalCount() + 1);
        state.setLastTransactionTimestamp(eventTime);
        state.setLastCountry(event.country());
        store.put(accountId, state);

        AccountFeatureVector feature =
                new AccountFeatureVector(accountId, recentCount, amountRatio, lastTxGapSec, countryChanged);
        log.info("계좌 피처 갱신: accountId={}, recent{}MinCount={}, amountRatio={}, lastTxGapSec={}, countryChanged={}",
                accountId, recentWindow.toMinutes(), feature.recent5MinCount(), feature.amountRatio(),
                feature.lastTxGapSec(), feature.countryChanged());

        context.forward(record.withValue(feature));
    }

    /** 윈도우보다 오래된 타임스탬프를 트리밍하고, 이번 거래를 추가한 뒤 개수를 반환한다. */
    private int updateRecentWindowCount(AccountActivityState state, Instant eventTime) {
        Instant windowStart = eventTime.minus(recentWindow);
        var recentTimestamps = state.getRecentTimestamps();
        while (!recentTimestamps.isEmpty() && recentTimestamps.peekFirst().isBefore(windowStart)) {
            recentTimestamps.pollFirst();
        }
        recentTimestamps.addLast(eventTime);
        return recentTimestamps.size();
    }

    /**
     * "평소 금액"(이번 거래 이전까지의 전체 기간 단순 평균) 대비 이번 거래 금액의 배율.
     * 비교 대상이 없는 첫 거래(또는 과거 평균이 0인 경우)는 1.0(평소와 동일)으로 처리한다.
     *
     * 트레이드오프: 정교하게 하려면 이동평균(EWMA)이나 최근 N분 윈도우 평균을 써서 계좌의 최근
     * 소비 패턴 변화에 더 민감하게 반응해야 하지만, 계좌가 오래될수록 평균이 둔감해지는 한계를
     * 감수하고 MVP는 전체 기간 누적 평균으로 시작한다 (다음 개선 후보, docs/BACKEND.md 참고).
     */
    private double calculateAmountRatio(AccountActivityState state, BigDecimal currentAmount) {
        if (state.getTotalCount() == 0) {
            return 1.0;
        }
        BigDecimal usualAmount = state.getTotalAmount()
                .divide(BigDecimal.valueOf(state.getTotalCount()), MathContext.DECIMAL64);
        if (usualAmount.signum() == 0) {
            return 1.0;
        }
        return currentAmount.divide(usualAmount, MathContext.DECIMAL64).doubleValue();
    }

    /** 직전 거래 이후 경과 시간(초). 첫 거래는 비교 대상이 없으므로 null. */
    private Long calculateGapSeconds(AccountActivityState state, Instant eventTime) {
        if (state.getLastTransactionTimestamp() == null) {
            return null;
        }
        return Duration.between(state.getLastTransactionTimestamp(), eventTime).getSeconds();
    }
}
