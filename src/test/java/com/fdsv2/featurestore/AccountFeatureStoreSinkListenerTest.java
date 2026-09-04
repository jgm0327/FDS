package com.fdsv2.featurestore;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * 실제 Redis 없이, StringRedisTemplate을 모킹해서 "어떤 키/값/TTL로 SET을 호출하는지"만 검증한다.
 * 실제 Redis 대상 e2e 검증은 docker-compose(CP3)로 별도 수행 (세션 로그 참고).
 */
@ExtendWith(MockitoExtension.class)
class AccountFeatureStoreSinkListenerTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private AccountFeatureStoreSinkListener listener;

    @BeforeEach
    void setUp() {
        listener = new AccountFeatureStoreSinkListener(redisTemplate);
        ReflectionTestUtils.setField(listener, "keyPrefix", "feature:account:");
        ReflectionTestUtils.setField(listener, "ttlMinutes", 30L);
    }

    @Test
    void 계좌ID를_키로_JSON_원문을_TTL과_함께_저장한다() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        String featureJson = "{\"accountId\":\"acc-1\",\"recentWindowCount\":3}";

        listener.onFeatureUpdate(new ConsumerRecord<>("account-feature-updates", 0, 0, "acc-1", featureJson));

        verify(valueOperations).set(eq("feature:account:acc-1"), eq(featureJson), eq(Duration.ofMinutes(30)));
    }

    @Test
    void 키나_값이_없는_레코드는_저장하지_않고_건너뛴다() {
        listener.onFeatureUpdate(new ConsumerRecord<>("account-feature-updates", 0, 0, null, "some-json"));

        verify(redisTemplate, never()).opsForValue();
    }

    @Test
    void 같은_계좌의_새_피처는_기존_값을_덮어쓴다() {
        // Redis SET은 원래 덮어쓰기 동작이라 별도 분기가 없다 — 두 번 호출해도 같은 키로 SET이
        // 두 번 나가는 것만 확인하면 "덮어쓰기"라는 스펙(append 아님, docs/ARCHITECTURE.md 3번)을
        // 만족한다는 걸 보증할 수 있다.
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        listener.onFeatureUpdate(new ConsumerRecord<>("account-feature-updates", 0, 0, "acc-1", "{\"recentWindowCount\":1}"));
        listener.onFeatureUpdate(new ConsumerRecord<>("account-feature-updates", 0, 1, "acc-1", "{\"recentWindowCount\":2}"));

        verify(valueOperations).set(eq("feature:account:acc-1"), eq("{\"recentWindowCount\":1}"), any(Duration.class));
        verify(valueOperations).set(eq("feature:account:acc-1"), eq("{\"recentWindowCount\":2}"), any(Duration.class));
    }

    @Test
    void redis_쓰기_실패시_예외를_삼키지_않고_그대로_전파한다() {
        // 코드 리뷰 지적: 에러 처리가 전혀 없어서 Redis 장애 시 레코드가 조용히 유실되던 문제.
        // 여기서 예외를 삼키지 않고 그대로 던져야 FeatureStoreKafkaConfig의 DefaultErrorHandler가
        // 재시도할 기회를 얻는다 — 리스너 메서드 안에서 조용히 catch하면 재시도 자체가 불가능해진다.
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        RuntimeException redisFailure = new RuntimeException("Redis connection refused");
        org.mockito.Mockito.doThrow(redisFailure).when(valueOperations)
                .set(any(String.class), any(String.class), any(Duration.class));

        assertThatThrownBy(() -> listener.onFeatureUpdate(
                new ConsumerRecord<>("account-feature-updates", 0, 0, "acc-1", "{\"recentWindowCount\":1}")))
                .isSameAs(redisFailure);
    }
}
