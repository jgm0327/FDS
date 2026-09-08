package com.fdsv2.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.fdsv2.modelclient.AccountRecentSequenceReader;
import com.fdsv2.modelclient.RawFeatureStep;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FraudDecisionControllerTest {

    @Mock
    private AccountRecentSequenceReader sequenceReader;

    @Mock
    private FraudDecisionService fraudDecisionService;

    private FraudDecisionController controller;

    @BeforeEach
    void setUp() {
        controller = new FraudDecisionController(sequenceReader, fraudDecisionService);
    }

    @Test
    void 최근_시퀀스의_마지막_스텝으로_판정을_요청한다() {
        RawFeatureStep step1 = new RawFeatureStep("acc-1", 1, 1.0, null, false, "GROCERY");
        RawFeatureStep step2 = new RawFeatureStep("acc-1", 2, 3.0, 100L, false, "GROCERY");
        when(sequenceReader.readRecentSteps("acc-1")).thenReturn(List.of(step1, step2));
        FraudDecision expected =
                new FraudDecision("acc-1", Action.ALLOW, 0.1, 0.1, "MODEL", 0.1, List.of(), Instant.now());
        when(fraudDecisionService.decide("acc-1", step2)).thenReturn(expected);

        FraudDecision result = controller.getFraudDecision("acc-1");

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void 이력이_없는_계좌는_null_스텝으로_판정을_요청한다() {
        when(sequenceReader.readRecentSteps("acc-none")).thenReturn(List.of());
        FraudDecision expected =
                new FraudDecision("acc-none", Action.ALLOW, 0.5, null, null, 0.5, List.of(), Instant.now());
        when(fraudDecisionService.decide("acc-none", null)).thenReturn(expected);

        FraudDecision result = controller.getFraudDecision("acc-none");

        assertThat(result).isEqualTo(expected);
    }
}
