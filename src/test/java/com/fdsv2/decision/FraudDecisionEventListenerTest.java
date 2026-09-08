package com.fdsv2.decision;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fdsv2.featurestore.FeatureStoreUpdatedEvent;
import com.fdsv2.modelclient.RawFeatureStep;
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

    private FraudDecisionEventListener listener;

    @BeforeEach
    void setUp() {
        listener = new FraudDecisionEventListener(fraudDecisionService);
    }

    @Test
    void 피처갱신_이벤트를_파싱해서_판정을_트리거한다() {
        String json = "{\"accountId\":\"acc-1\",\"recentWindowCount\":3,\"amountRatio\":1.1,"
                + "\"lastTxGapSec\":100,\"countryChanged\":false,\"merchantCategory\":\"GROCERY\"}";

        listener.onFeatureStoreUpdated(new FeatureStoreUpdatedEvent("acc-1", json, Instant.now()));

        verify(fraudDecisionService).decide(eq("acc-1"), any(RawFeatureStep.class));
    }

    @Test
    void JSON_파싱이_실패해도_예외를_밖으로_던지지_않고_판정도_트리거하지_않는다() {
        assertThatCode(() -> listener.onFeatureStoreUpdated(
                new FeatureStoreUpdatedEvent("acc-1", "not-json", Instant.now())))
                .doesNotThrowAnyException();

        verify(fraudDecisionService, never()).decide(eq("acc-1"), any());
    }
}
