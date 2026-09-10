package com.fdsv2.featurestore;

import java.time.Duration;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
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
 *
 * <p>최근 거래 시퀀스(CP4 연동, backend/sequence-window-feature-store): 스냅샷 SET과 별개로,
 * 같은 JSON을 계좌별 Redis LIST({@code feature:account:{id}:recent})에도 RPUSH한다.
 * account-feature-updates는 거래 1건당 메시지 1개이고 각 메시지의 amountRatio/lastTxGapSec/
 * countryChanged/merchantCategory가 전부 "그 거래 자체"의 스텝 값이므로(AccountFeatureVector
 * 참고), 이 스트림을 원문 그대로 LIST에 쌓기만 해도 CP4가 요구하는 "계좌의 최근 거래 시퀀스"가
 * 만들어진다 — CP2 State Store나 이 리스너의 JSON-passthrough 원칙을 전혀 건드리지 않는 최소
 * 변경으로 간극을 메운 것 (세션 로그의 "CP2/CP3 ↔ CP4 인터페이스 간극" 논의 참고).
 *
 * <p>CP5 연동(backend/decision-ensemble): Redis 쓰기가 전부 끝난 직후 {@link FeatureStoreUpdatedEvent}를
 * 발행한다 — CP5의 {@code FraudDecisionEventListener}가 이 이벤트를 구독해 자동 판정을 트리거한다.
 * 이 리스너는 이벤트를 "발행"만 하고 구독자가 누구인지, 몇 명인지 몰라도 된다 (자세한 이유는
 * {@link FeatureStoreUpdatedEvent} 클래스 javadoc 참고).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccountFeatureStoreSinkListener {

    private final StringRedisTemplate redisTemplate;
    private final FeatureStoreKeyBuilder keyBuilder;
    private final ApplicationEventPublisher eventPublisher;

    @Value("${fds.feature-store.ttl-minutes}")
    private long ttlMinutes;

    // CP4(ai/pytorch-sequence-model)의 MAX_SEQ_LEN과 반드시 같은 값이어야 한다 — 이 값이 더 크면
    // Redis에 불필요하게 오래된 이력이 쌓이고, 더 작으면 CP4가 원하는 만큼의 컨텍스트를 못 준다.
    // 두 저장소가 서로 다른 언어/레포 경계에 있어 컴파일 타임에 맞출 방법이 없으므로, 값을 바꿀
    // 때는 항상 두 쪽을 함께 바꿔야 한다(ai/README.md에도 동일하게 문서화).
    @Value("${fds.feature-store.recent-window-size}")
    private int recentWindowSize;

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

        Duration ttl = Duration.ofMinutes(ttlMinutes);

        String snapshotKey = keyBuilder.key(accountId);
        redisTemplate.opsForValue().set(snapshotKey, featureJson, ttl);
        log.debug("Redis 피처 스토어 갱신: key={}, ttlMinutes={}", snapshotKey, ttlMinutes);

        // RPUSH로 맨 뒤에 추가 후 앞쪽(오래된 것)을 잘라 최근 recentWindowSize건만 유지.
        // 스냅샷과 마찬가지로 거래가 들어올 때마다 TTL을 리셋한다 — 활발한 계좌는 사실상 안 만료됨.
        String recentKey = keyBuilder.recentKey(accountId);
        redisTemplate.opsForList().rightPush(recentKey, featureJson);
        redisTemplate.opsForList().trim(recentKey, -recentWindowSize, -1);
        redisTemplate.expire(recentKey, ttl);
        log.debug("Redis 최근 거래 시퀀스 갱신: key={}, recentWindowSize={}", recentKey, recentWindowSize);

        // CP5(판정 및 대응) 자동 트리거 — 위 Redis 쓰기가 전부 끝난 "이후"에 발행해야, CP5가 모델
        // 호출 시 이번 거래까지 반영된 최신 시퀀스를 읽는다(클래스 javadoc/FeatureStoreUpdatedEvent
        // 참고). 구독자(CP5)는 전용 스레드풀에서 비동기로 처리하고 자기 예외를 스스로 삼키지만
        // (FraudDecisionEventListener 참고), 이 publishEvent 호출 자체(리스너 조회/디스패치)에서
        // 예외가 날 가능성까지 방어적으로 한 번 더 막는다 — 여기서 예외가 새면 이미 성공한 Redis
        // 쓰기가 있는 레코드가 DefaultErrorHandler에 의해 불필요하게 재시도(중복 RPUSH)될 수 있다.
        try {
            eventPublisher.publishEvent(new FeatureStoreUpdatedEvent(accountId, featureJson, Instant.now()));
        } catch (Exception e) {
            log.warn("CP5 자동 판정 트리거 이벤트 발행 실패 — Redis 쓰기는 이미 완료됨, 판정만 스킵: accountId={}",
                    accountId, e);
        }
    }
}
