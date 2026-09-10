package com.fdsv2.sequence;

import com.fdsv2.transaction.AccountShardKey;
import com.fdsv2.transaction.TransactionEvent;
import java.time.Duration;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * (backend/kafka-salting) CP2 Stage 1 — 샤드별 병렬 처리.
 *
 * <p><b>왜 이 단계가 필요한가</b>: CP1 세션(2026-09-10 e2e 검증, docs/sessions 참고)에서 핫
 * 파티션 상황을 실측으로 재현했는데, 컨슈머 인스턴스를 늘려도 안 풀리는 문제였다 — Kafka는
 * 파티션 하나를 그룹 내 스레드 하나만 처리할 수 있어서다. 이 프로세서는 "accountId#shardIndex"
 * 형식의 샤드 키(입력 메시지 키, {@code com.fdsv2.transaction.TransactionEventProducer} 참고)
 * 별로 <b>독립된</b> State Store 엔트리를
 * 유지한다. 고빈도 계좌는 이 샤드가 여러 개(각각 다른 파티션 -> 다른 Streams 태스크 -> 다른
 * 스레드)라서, 이 단계의 무거운 작업(슬라이딩 윈도우 타임스탬프 트리밍 + RocksDB get/put)이 여러
 * 스레드로 병렬 분산된다.
 *
 * <p>이 단계는 "계좌 전체 관점"의 최종 값(amountRatio, lastTxGapSec, countryChanged)을 계산하지
 * 않는다 — 그건 이 샤드 하나만 봐서는 알 수 없고(다른 샤드에 더 최근 거래가 있을 수 있음), Stage
 * 2({@link AccountActivityMergeProcessor})가 여러 샤드의 부분 상태를 병합해서 계산한다. 이
 * 단계는 딱 "이 샤드로 들어온 거래만 놓고 본" 부분 집계({@link PartialAccountFeatureVector})만
 * 만들어서 넘긴다.
 *
 * <p>일반 계좌(고빈도로 지정 안 됨)는 항상 샤드가 1개("accountId#0")뿐이라, 이 샤드의 부분
 * 상태가 곧 그 계좌의 전체 상태와 같다 — Stage 2에서 병합해도 결과가 달라지지 않는다
 * (AccountActivityMergeProcessorTest, 그리고 기존 AccountActivityProcessorTest가 그대로
 * 회귀 테스트로 통과하는 것으로 검증).
 */
@Slf4j
public class ShardedAccountActivityProcessor
        implements FixedKeyProcessor<String, TransactionEvent, PartialAccountFeatureVector> {

    private final String storeName;
    private final Duration recentWindow;

    private FixedKeyProcessorContext<String, PartialAccountFeatureVector> context;
    private KeyValueStore<String, ShardActivityState> store;

    public ShardedAccountActivityProcessor(String storeName, Duration recentWindow) {
        this.storeName = storeName;
        this.recentWindow = recentWindow;
    }

    @Override
    public void init(FixedKeyProcessorContext<String, PartialAccountFeatureVector> context) {
        this.context = context;
        this.store = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, TransactionEvent> record) {
        String shardKeyRaw = record.key();
        AccountShardKey shardKey = AccountShardKey.parse(shardKeyRaw);
        TransactionEvent event = record.value();

        // 컨트롤러가 요청 바디를 검증하지 않으므로(@Valid 없음), occurredAt/amount가 null인 채로
        // 여기까지 올 수 있다 — 옛 AccountActivityProcessor와 동일한 방어(예외를 던지면
        // StreamsUncaughtExceptionHandler가 없어서 스트림 스레드 전체가 죽는다).
        if (event.occurredAt() == null || event.amount() == null) {
            log.warn("잘못된 거래 이벤트라 건너뜀 (occurredAt/amount 누락): accountId={}, shardIndex={}, transactionId={}",
                    shardKey.accountId(), shardKey.shardIndex(), event.transactionId());
            return;
        }

        Instant eventTime = event.occurredAt();

        ShardActivityState state = store.get(shardKeyRaw);
        if (state == null) {
            state = new ShardActivityState();
        }

        Instant windowStart = eventTime.minus(recentWindow);
        var recentTimestamps = state.getRecentTimestamps();
        recentTimestamps.removeIf(ts -> ts.isBefore(windowStart));
        recentTimestamps.addLast(eventTime);
        int shardRecentWindowCount = recentTimestamps.size();

        var newTotalAmount = state.getTotalAmount().add(event.amount());
        long newTotalCount = state.getTotalCount() + 1;
        state.setTotalAmount(newTotalAmount);
        state.setTotalCount(newTotalCount);
        store.put(shardKeyRaw, state);

        PartialAccountFeatureVector partial = new PartialAccountFeatureVector(
                shardKey.accountId(), shardKey.shardIndex(), shardRecentWindowCount,
                newTotalAmount, newTotalCount, event.amount(), eventTime, event.country(), event.merchantCategory());

        // CP1 k6 시나리오처럼 계좌 하나에 초당 수백 건이 몰리는 상황에서 INFO 레벨 로깅 자체가
        // 병목이 될 수 있어 debug로 낮췄다 (옛 AccountActivityProcessor와 동일한 판단).
        log.debug("샤드 부분 집계 갱신: accountId={}, shardIndex={}, shardRecentWindowCount={}",
                shardKey.accountId(), shardKey.shardIndex(), shardRecentWindowCount);

        context.forward(record.withValue(partial));
    }
}
