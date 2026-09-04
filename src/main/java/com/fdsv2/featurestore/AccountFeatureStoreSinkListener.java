package com.fdsv2.featurestore;

import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * CP3 — 온라인 피처 스토어. CP2가 발행하는 account-feature-updates 토픽(키=accountId,
 * 값=AccountFeatureVector의 JSON)을 그대로 Redis에 옮겨 담는다 (docs/ARCHITECTURE.md 3번).
 *
 * 값은 CP2의 {@code com.fdsv2.sequence.AccountFeatureVector} 자바 타입으로 역직렬화하지 않고
 * 원문 JSON 문자열 그대로 받아서 그대로 저장한다 — CP3가 CP2의 도메인 클래스에 의존하지 않고
 * "토픽에 담긴 JSON 형태"만 계약으로 삼기 위함이다 (병렬 worktree 원칙, docs/WORKTREE_SETUP.md).
 * CP2가 필드를 추가/변경해도 CP3는 재컴파일 없이 그대로 통과시킨다.
 *
 * 계좌ID별로 항상 같은 파티션에서 오므로(CP1), 같은 계좌의 최신 피처가 옛날 피처를 덮어쓰는 순서가
 * 보장된다 — Redis SET이 append가 아니라 "최종 상태 덮어쓰기"인 이 스펙과 정확히 맞아떨어진다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccountFeatureStoreSinkListener {

    private final StringRedisTemplate redisTemplate;

    @Value("${fds.feature-store.key-prefix}")
    private String keyPrefix;

    @Value("${fds.feature-store.ttl-minutes}")
    private long ttlMinutes;

    // 코드 리뷰 지적: account-feature-updates는 32개 파티션인데 concurrency 지정이 없으면 Spring
    // Kafka가 스레드 하나로 32개 파티션을 전부 처리한다 — CP1/CP2가 파티션 병렬성을 전제로 설계된
    // 것과 어긋난다. 인스턴스 하나에서 무작정 32로 올리면 오히려 과할 수 있어, 실측 후 조정 가능한
    // 값으로 4를 기본값으로 잡았다(CP1의 "넉넉하게 시작 → 실측 후 조정" 원칙과 동일, 다음 개선 후보).
    @KafkaListener(
            topics = "${fds.kafka.feature-updates.topic-name}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${fds.feature-store.consumer-concurrency:4}")
    public void onFeatureUpdate(ConsumerRecord<String, String> record) {
        String accountId = record.key();
        String featureJson = record.value();

        if (accountId == null || featureJson == null) {
            log.warn("잘못된 피처 업데이트 레코드라 건너뜀: key={}, value={}", accountId, featureJson);
            return;
        }

        String redisKey = keyPrefix + accountId;
        redisTemplate.opsForValue().set(redisKey, featureJson, Duration.ofMinutes(ttlMinutes));
        log.debug("Redis 피처 스토어 갱신: key={}, ttlMinutes={}", redisKey, ttlMinutes);
    }
}
