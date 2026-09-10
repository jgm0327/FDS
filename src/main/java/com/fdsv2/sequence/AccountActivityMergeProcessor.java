package com.fdsv2.sequence;

import com.fdsv2.sequence.AccountMergeState.ShardSnapshot;
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
 * (backend/kafka-salting) CP2 Stage 2 — 계좌 단위 재집계("salting 후 재집계",
 * docs/ARCHITECTURE.md 1번 "핫 파티션 문제" 대응 1 그대로).
 *
 * Stage 1({@link ShardedAccountActivityProcessor})이 만든 {@link PartialAccountFeatureVector}를
 * accountId로 재파티션(topology의 {@code selectKey}+{@code repartition})한 뒤 받는다 — 같은
 * 계좌의 모든 샤드가 여기서는 다시 하나의 Streams 태스크로 모인다. 이 단계는 계좌마다 "각 샤드가
 * 마지막으로 보고한 값"만 작은 State Store({@link AccountMergeState})에 들고 있다가, 매 부분
 * 집계가 들어올 때마다 전역 값을 재계산해서 옛 {@link AccountFeatureVector}와 완전히 같은 형태로
 * 내보낸다 — CP3/CP4/CP5는 이 단계가 있는지조차 몰라도 된다.
 *
 * <p><b>정확한 것과 근사인 것</b>:
 * <ul>
 *   <li>{@code amountRatio} 분모(전역 평균) — <b>정확함</b>. "여러 값의 합"이라 어느 순서로
 *       합쳐도 결과가 같다(교환법칙) — 샤드가 몇 개든, 어떤 순서로 도착하든 최종 합계는 항상
 *       같다. totalAmount/totalCount는 전체 기간 누적값이라 "오래된 샤드"라는 개념 자체가 없다.</li>
 *   <li>{@code recentWindowCount} — 샤드별 값의 합이지만, "윈도우"라는 시간 개념이 있어서 그냥
 *       더하기만 하면 안 된다(코드 리뷰 지적, 아래 "왜 샤드 스냅숏에 시각을 추가했는가" 참고) —
 *       트래픽이 끊긴 샤드의 옛 값을 윈도우 밖으로 판단해서 제외해야 정확하다.</li>
 *   <li>{@code lastTxGapSec}, {@code countryChanged} — <b>근사값</b>. "전역에서 가장 최근 거래"가
 *       필요한데, 샤드마다 도착 타이밍이 조금씩 달라서 이 단계가 그 시점에 "알고 있는" 가장 최근
 *       값을 쓴다. 옛(단일 계좌) 집계와 동일한 원칙(더 이른 이벤트가 오면 기준점을 덮어쓰지
 *       않음)으로 최소한 "역행"은 막지만, 다른 샤드의 이벤트가 아직 도착 전이면 그 시점 기준으로는
 *       놓칠 수 있다. 고빈도 계좌만 이 트레이드오프를 감수한다 — 샤드가 1개뿐인 일반 계좌는 이
 *       문제 자체가 발생하지 않는다(항상 정확).</li>
 * </ul>
 *
 * <p><b>코드 리뷰 반영 — 왜 샤드 스냅숏에 시각을 추가했는가</b>: 처음 구현은
 * {@code recentWindowCount}를 "각 샤드가 마지막으로 보고한 값의 합"으로만 계산했다. 그런데 한
 * 샤드가 트래픽을 계속 받다가 갑자기 멈추면, 그 샤드의 마지막 스냅숏(윈도우 안에서 계산된 값)이
 * {@link AccountMergeState}에 영원히 남아서 다른 샤드들이 계속 활동해도 계좌 전체의
 * recentWindowCount가 그만큼 계속 부풀려진다 — 리뷰가 구체적 재현 시나리오로 지적한 실제 버그.
 * {@link AccountMergeState.ShardSnapshot#getLastEventTime()}을 기준으로, 병합 시점에 그 샤드가
 * "지금 이 거래 기준으로 윈도우 안에서 마지막으로 보고했는지"를 확인해서, 윈도우 밖이면 그
 * 샤드의 기여분을 0으로 취급한다.
 */
@Slf4j
public class AccountActivityMergeProcessor
        implements FixedKeyProcessor<String, PartialAccountFeatureVector, AccountFeatureVector> {

    private final String storeName;
    private final Duration recentWindow;

    private FixedKeyProcessorContext<String, AccountFeatureVector> context;
    private KeyValueStore<String, AccountMergeState> store;

    public AccountActivityMergeProcessor(String storeName, Duration recentWindow) {
        this.storeName = storeName;
        this.recentWindow = recentWindow;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, AccountFeatureVector> context) {
        this.context = context;
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, PartialAccountFeatureVector> record) {
        String accountId = record.key();
        PartialAccountFeatureVector partial = record.value();

        AccountMergeState state = store.get(accountId);
        if (state == null) {
            state = new AccountMergeState();
        }

        // 이번 거래 "이전"까지의 전역 합계/건수 — 이 샤드는 갱신되기 전 마지막 스냅숏을 쓰고(아직
        // 이번 거래가 반영 안 된 값), 다른 샤드는 각자 마지막으로 보고한(이미 최신인) 스냅숏을
        // 그대로 쓴다. totalAmount/totalCount는 전체 기간 누적이라 "오래됐다"는 개념이 없어서
        // 시각 확인 없이 그대로 더한다.
        BigDecimal globalTotalBefore = BigDecimal.ZERO;
        long globalCountBefore = 0;
        int globalRecentWindowCount = 0;
        Instant windowStart = partial.occurredAt().minus(recentWindow);
        for (var entry : state.getShardSnapshots().entrySet()) {
            if (entry.getKey() == partial.shardIndex()) {
                continue; // 이 샤드는 아래서 별도로(이번 거래 포함 전 값으로) 더한다.
            }
            ShardSnapshot other = entry.getValue();
            globalTotalBefore = globalTotalBefore.add(other.getTotalAmount());
            globalCountBefore += other.getTotalCount();
            // 코드 리뷰 지적: recentWindowCount는 "윈도우" 개념이 있는 값이라, 이 샤드가 마지막으로
            // 보고한 시각이 지금(이번 거래) 기준 윈도우 밖이면 그 값은 더 이상 유효하지 않다 — 트래픽이
            // 끊긴 샤드의 옛 값이 영원히 합계에 남아 계좌 전체 windowCount를 계속 부풀리는 버그 방지.
            if (other.getLastEventTime() != null && !other.getLastEventTime().isBefore(windowStart)) {
                globalRecentWindowCount += other.getRecentWindowCount();
            }
        }
        ShardSnapshot previousOwnSnapshot = state.getShardSnapshots().get(partial.shardIndex());
        if (previousOwnSnapshot != null) {
            globalTotalBefore = globalTotalBefore.add(previousOwnSnapshot.getTotalAmount());
            globalCountBefore += previousOwnSnapshot.getTotalCount();
        }
        // 이 샤드의 recentWindowCount는 Stage 1이 "지금 이 거래" 기준으로 이미 윈도우 트리밍까지
        // 마쳐서 보냈으므로(즉 항상 최신), 시각 확인 없이 그대로 합산한다.
        globalRecentWindowCount += partial.shardRecentWindowCount();

        double amountRatio = calculateAmountRatio(globalTotalBefore, globalCountBefore, partial.amount());
        Long lastTxGapSec = state.getLastTransactionTimestamp() == null
                ? null
                : Duration.between(state.getLastTransactionTimestamp(), partial.occurredAt()).getSeconds();
        boolean countryChanged = state.getLastCountry() != null && !state.getLastCountry().equals(partial.country());

        // 이 샤드의 스냅숏을 "이번 거래 포함"(Stage 1이 이미 계산해 보낸 값)으로 갱신 — 이 거래의
        // 시각도 함께 저장해서, 다음번 다른 샤드가 병합할 때 이 스냅숏이 아직 윈도우 안인지
        // 판단할 수 있게 한다.
        state.getShardSnapshots().put(partial.shardIndex(),
                new ShardSnapshot(partial.shardTotalAmount(), partial.shardTotalCount(),
                        partial.shardRecentWindowCount(), partial.occurredAt()));

        // 전역 "가장 최근" 갱신 — 역전 이벤트가 오면 덮어쓰지 않는다(옛 AccountActivityProcessor와
        // 동일한 방어 원칙, 여러 샤드로 확장).
        if (state.getLastTransactionTimestamp() == null
                || !partial.occurredAt().isBefore(state.getLastTransactionTimestamp())) {
            state.setLastTransactionTimestamp(partial.occurredAt());
            state.setLastCountry(partial.country());
        }

        store.put(accountId, state);

        AccountFeatureVector feature = new AccountFeatureVector(
                accountId, globalRecentWindowCount, amountRatio, lastTxGapSec, countryChanged,
                partial.merchantCategory());

        log.debug("계좌 재집계 완료: accountId={}, shardIndex={}, recentWindowCount={}, amountRatio={}",
                accountId, partial.shardIndex(), feature.recentWindowCount(), feature.amountRatio());

        context.forward(record.withValue(feature));
    }

    /**
     * "평소 금액"(이번 거래 이전까지의 전역 단순 평균) 대비 이번 거래 금액의 배율 — 옛
     * AccountActivityProcessor.calculateAmountRatio와 동일한 규칙(첫 거래/평균이 0 이하면 1.0).
     */
    private double calculateAmountRatio(BigDecimal totalBefore, long countBefore, BigDecimal currentAmount) {
        if (countBefore == 0) {
            return 1.0;
        }
        BigDecimal usualAmount = totalBefore.divide(BigDecimal.valueOf(countBefore), MathContext.DECIMAL64);
        if (usualAmount.signum() <= 0) {
            return 1.0;
        }
        return currentAmount.divide(usualAmount, MathContext.DECIMAL64).doubleValue();
    }
}
