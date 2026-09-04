package com.fdsv2.modelclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdsv2.featurestore.FeatureStoreKeyBuilder;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * backend/sequence-window-feature-store가 유지하는 {@code feature:account:{id}:recent} Redis
 * LIST를 읽어 CP4가 요구하는 "계좌의 최근 거래 시퀀스"로 파싱한다.
 *
 * 값이 없거나(신규/비활성 계좌) 파싱에 실패한 원소가 있어도 예외를 던지지 않고 최대한 나머지로
 * 진행한다 — 이 메서드의 결과가 비어있으면 호출부(TorchServeModelInferenceClient)가 빈 시퀀스로
 * 모델을 호출하거나(모델이 빈 입력을 어떻게 다루는지는 handler.py 쪽 책임) 폴백으로 넘어간다.
 */
@Slf4j
@Component
public class AccountRecentSequenceReader {

    private final StringRedisTemplate redisTemplate;
    private final FeatureStoreKeyBuilder keyBuilder;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public AccountRecentSequenceReader(StringRedisTemplate redisTemplate, FeatureStoreKeyBuilder keyBuilder) {
        this.redisTemplate = redisTemplate;
        this.keyBuilder = keyBuilder;
    }

    public List<RawFeatureStep> readRecentSteps(String accountId) {
        List<String> rawJsonList = redisTemplate.opsForList().range(keyBuilder.recentKey(accountId), 0, -1);
        if (rawJsonList == null || rawJsonList.isEmpty()) {
            return List.of();
        }
        return rawJsonList.stream()
                .map(this::parseOrNull)
                .filter(Objects::nonNull)
                .toList();
    }

    private RawFeatureStep parseOrNull(String json) {
        try {
            return objectMapper.readValue(json, RawFeatureStep.class);
        } catch (Exception e) {
            log.warn("최근 거래 시퀀스 항목 파싱 실패, 건너뜀: json={}, cause={}", json, e.toString());
            return null;
        }
    }
}
