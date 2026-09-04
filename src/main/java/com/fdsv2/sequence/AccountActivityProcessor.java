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
 * accountId가 파티션 키이므로(CP1), 같은 계좌의 이벤트는 대체로 순서대로 도착하지만, 클라이언트
 * 시계 오차나 재시도로 occurredAt이 역전된 이벤트가 섞여 들어올 수 있다는 걸 전제로 방어적으로
 * 짰다 — 코드 리뷰 중 TopologyTestDriver로 "역전된 이벤트가 recent5MinCount를 영구적으로 부풀리고,
 * lastTxGapSec 오염이 이후 정상 이벤트에도 전파되는" 버그를 실제로 재현해서 아래 두 지점에서
 * 방어 로직을 넣었다:
 *   1) updateRecentWindowCount: 앞에서만 트리밍하지 않고 윈도우 밖 항목을 전부 제거 (역전 이벤트가
 *      뒤에 남아 카운트를 영구히 부풀리는 걸 방지)
 *   2) process(): lastTransactionTimestamp/lastCountry는 "지금까지 본 것 중 가장 최근" 값만 갱신
 *      (역전된 이벤트가 기준점 자체를 오염시켜서 다음 이벤트의 gap 계산까지 틀어지는 걸 방지)
 *
 * 여전히 다루지 않는 것(다음 개선 후보, docs/BACKEND.md 참고):
 *   - transactionId 중복 제거 — 프로듀서 멱등성(enable.idempotence)은 브로커 재시도 중복만 막고,
 *     애플리케이션 레벨 재시도(같은 transactionId로 재발행)는 그대로 이중 집계된다.
 *   - occurredAt이 역전된 "그 이벤트 자체"의 recent5MinCount/amountRatio 값 자체가 정확하다는
 *     보장은 없다 (역전 이벤트가 상태를 오염시키지 않게 막을 뿐).
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

        // 컨트롤러가 요청 바디를 검증하지 않으므로(@Valid 없음), occurredAt/amount가 null인 채로
        // 여기까지 올 수 있다. 예외를 던지면 StreamsUncaughtExceptionHandler가 없어서 스트림
        // 스레드 전체가 죽어 다른 모든 계좌 처리까지 멈추므로, 이 레코드만 건너뛴다.
        if (event.occurredAt() == null || event.amount() == null) {
            log.warn("잘못된 거래 이벤트라 건너뜀 (occurredAt/amount 누락): accountId={}, transactionId={}",
                    accountId, event.transactionId());
            return;
        }

        Instant eventTime = event.occurredAt();

        AccountActivityState state = store.get(accountId);
        if (state == null) {
            state = new AccountActivityState();
        }

        int recentCount = updateRecentWindowCount(state, eventTime);
        double amountRatio = calculateAmountRatio(state, event.amount());
        Long lastTxGapSec = calculateGapSeconds(state, eventTime);
        boolean countryChanged = state.getLastCountry() != null && !state.getLastCountry().equals(event.country());

        // 다음 거래를 위해 상태 갱신. totalAmount/totalCount는 덧셈이라 순서 무관하게 안전하지만,
        // lastTransactionTimestamp/lastCountry는 "지금까지 본 것 중 가장 최근" 값이어야 하므로
        // 역전된(더 이른) 이벤트가 왔을 땐 덮어쓰지 않는다 — 안 그러면 다음 정상 이벤트의
        // lastTxGapSec/countryChanged까지 이 역전 이벤트를 기준으로 잘못 계산된다.
        state.setTotalAmount(state.getTotalAmount().add(event.amount()));
        state.setTotalCount(state.getTotalCount() + 1);
        if (state.getLastTransactionTimestamp() == null || !eventTime.isBefore(state.getLastTransactionTimestamp())) {
            state.setLastTransactionTimestamp(eventTime);
            state.setLastCountry(event.country());
        }
        store.put(accountId, state);

        AccountFeatureVector feature = new AccountFeatureVector(
                accountId, recentCount, amountRatio, lastTxGapSec, countryChanged, event.merchantCategory());
        // CP1 k6 시나리오처럼 계좌 하나에 초당 수백 건이 몰리는 상황에서 INFO 레벨 로깅 자체가
        // 병목이 될 수 있어 debug로 낮췄다 (필요할 때만 로그 레벨을 올려서 확인).
        log.debug("계좌 피처 갱신: accountId={}, recentWindowCount={}, amountRatio={}, lastTxGapSec={}, countryChanged={}",
                accountId, feature.recentWindowCount(), feature.amountRatio(),
                feature.lastTxGapSec(), feature.countryChanged());

        context.forward(record.withValue(feature));
    }

    /**
     * 윈도우보다 오래된 타임스탬프를 트리밍하고, 이번 거래를 추가한 뒤 개수를 반환한다.
     *
     * 앞쪽만 보고 트리밍하면(단순 FIFO) 역전된 이벤트가 뒤에 그대로 남아 윈도우를 영구히 벗어나지
     * 못하는 버그가 있었다 — removeIf로 위치에 상관없이 윈도우 밖 항목을 전부 제거한다.
     */
    private int updateRecentWindowCount(AccountActivityState state, Instant eventTime) {
        Instant windowStart = eventTime.minus(recentWindow);
        var recentTimestamps = state.getRecentTimestamps();
        recentTimestamps.removeIf(ts -> ts.isBefore(windowStart));
        recentTimestamps.addLast(eventTime);
        return recentTimestamps.size();
    }

    /**
     * "평소 금액"(이번 거래 이전까지의 전체 기간 단순 평균) 대비 이번 거래 금액의 배율.
     * 비교 대상이 없는 첫 거래, 과거 평균이 0이거나 음수인 경우(환불 등으로 누적 합계가 음수가 될
     * 수 있음)는 부호가 뒤집힌 의미 없는 비율이 나오는 걸 막기 위해 1.0(평소와 동일)으로 처리한다.
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
        if (usualAmount.signum() <= 0) {
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
