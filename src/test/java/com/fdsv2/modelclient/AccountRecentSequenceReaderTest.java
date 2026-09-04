package com.fdsv2.modelclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.fdsv2.featurestore.FeatureStoreKeyBuilder;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

@ExtendWith(MockitoExtension.class)
class AccountRecentSequenceReaderTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ListOperations<String, String> listOperations;

    private AccountRecentSequenceReader reader;

    @BeforeEach
    void setUp() {
        reader = new AccountRecentSequenceReader(redisTemplate, new FeatureStoreKeyBuilder("feature:account:"));
    }

    @Test
    void 리스트에_있는_JSON을_순서대로_파싱한다() {
        when(redisTemplate.opsForList()).thenReturn(listOperations);
        when(listOperations.range("feature:account:acc-1:recent", 0, -1)).thenReturn(List.of(
                "{\"accountId\":\"acc-1\",\"recentWindowCount\":1,\"amountRatio\":1.0,\"lastTxGapSec\":null,\"countryChanged\":false,\"merchantCategory\":\"GROCERY\"}",
                "{\"accountId\":\"acc-1\",\"recentWindowCount\":2,\"amountRatio\":2.0,\"lastTxGapSec\":30,\"countryChanged\":true,\"merchantCategory\":\"FUEL\"}"));

        List<RawFeatureStep> steps = reader.readRecentSteps("acc-1");

        assertThat(steps).hasSize(2);
        assertThat(steps.get(0).amountRatio()).isEqualTo(1.0);
        assertThat(steps.get(0).lastTxGapSec()).isNull();
        assertThat(steps.get(1).amountRatio()).isEqualTo(2.0);
        assertThat(steps.get(1).lastTxGapSec()).isEqualTo(30L);
        assertThat(steps.get(1).countryChanged()).isTrue();
        assertThat(steps.get(1).merchantCategory()).isEqualTo("FUEL");
    }

    @Test
    void 계좌_이력이_없으면_빈_리스트를_반환한다() {
        when(redisTemplate.opsForList()).thenReturn(listOperations);
        when(listOperations.range("feature:account:acc-none:recent", 0, -1)).thenReturn(null);

        assertThat(reader.readRecentSteps("acc-none")).isEmpty();
    }

    @Test
    void 손상된_항목은_건너뛰고_나머지는_반환한다() {
        when(redisTemplate.opsForList()).thenReturn(listOperations);
        when(listOperations.range("feature:account:acc-2:recent", 0, -1)).thenReturn(List.of(
                "{\"accountId\":\"acc-2\",\"amountRatio\":1.0,\"lastTxGapSec\":null,\"countryChanged\":false}",
                "이건-JSON이-아님",
                "{\"accountId\":\"acc-2\",\"amountRatio\":3.0,\"lastTxGapSec\":50,\"countryChanged\":false}"));

        List<RawFeatureStep> steps = reader.readRecentSteps("acc-2");

        assertThat(steps).hasSize(2);
        assertThat(steps.get(0).amountRatio()).isEqualTo(1.0);
        assertThat(steps.get(1).amountRatio()).isEqualTo(3.0);
    }
}
