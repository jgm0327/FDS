package com.fdsv2.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fdsv2.featurestore.FeatureStoreUpdatedEvent;
import com.fdsv2.modelclient.RawFeatureStep;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FraudDecisionEventListenerTest {

    @Mock
    private FraudDecisionService fraudDecisionService;

    private SimpleMeterRegistry meterRegistry;
    private FraudDecisionEventListener listener;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        listener = new FraudDecisionEventListener(fraudDecisionService, meterRegistry);
    }

    @Test
    void 피처갱신_이벤트를_파싱해서_판정을_트리거하고_e2e_latency를_기록한다() {
        String json = "{\"accountId\":\"acc-1\",\"recentWindowCount\":3,\"amountRatio\":1.1,"
                + "\"lastTxGapSec\":100,\"countryChanged\":false,\"merchantCategory\":\"GROCERY\"}";

        listener.onFeatureStoreUpdated(new FeatureStoreUpdatedEvent("acc-1", json, Instant.now()));

        verify(fraudDecisionService).decide(eq("acc-1"), any(RawFeatureStep.class));
        assertThat(meterRegistry.get("fds.decision.e2e.latency").timer().count()).isEqualTo(1L);
    }

    @Test
    void JSON_파싱이_실패해도_예외를_밖으로_던지지_않고_판정도_트리거하지_않는다() {
        assertThatCode(() -> listener.onFeatureStoreUpdated(
                new FeatureStoreUpdatedEvent("acc-1", "not-json", Instant.now())))
                .doesNotThrowAnyException();

        verify(fraudDecisionService, never()).decide(eq("acc-1"), any());
    }

    @Test
    void event_occurredAt이_미래_시각이라_음수_경과시간이_나와도_0으로_클램프해서_기록한다() {
        // 코드 리뷰 지적 회귀 테스트: 시스템 시계가 뒤로 튀는 등의 이유로 Duration.between(...)이
        // 음수가 나올 수 있는데, Micrometer Timer.record(Duration)은 음수를 조용히 버린다 —
        // 값을 잃지 않고 0으로 클램프해서 최소한 샘플 카운트는 남기는지 검증.
        String json = "{\"accountId\":\"acc-future\",\"recentWindowCount\":1,\"amountRatio\":1.0,"
                + "\"lastTxGapSec\":null,\"countryChanged\":false,\"merchantCategory\":\"GROCERY\"}";
        Instant future = Instant.now().plusSeconds(60);

        listener.onFeatureStoreUpdated(new FeatureStoreUpdatedEvent("acc-future", json, future));

        assertThat(meterRegistry.get("fds.decision.e2e.latency").timer().count()).isEqualTo(1L);
        assertThat(meterRegistry.get("fds.decision.e2e.latency").timer().totalTime(java.util.concurrent.TimeUnit.NANOSECONDS))
                .isEqualTo(0.0);
    }
}
