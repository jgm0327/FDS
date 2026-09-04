package com.fdsv2.featurestore;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * CP3 패널 확장(측정 전용) — Redis GET latency/캐시 히트율(docs/PERFORMANCE_MEASUREMENT.md CP3
 * 표)을 관측하려면 실제로 GET을 하는 컴포넌트가 있어야 하는데, 아직 없다 (ARCHITECTURE.md 파이프
 * 라인상 "Redis 조회"는 CP4/모델 서빙의 역할). CP3 구현 세션에서는 "이번 범위에서 안 만듦"이라고
 * 명시적으로 미뤘던 것과 같은 엔드포인트지만, 지표를 눈으로 확인하려면 k6가 때릴 대상이 있어야
 * 해서 이번 관측 확장 브랜치에서 "측정 전용"으로 최소하게 추가한다.
 *
 * CP4가 실제 모델 서빙 로직에서 이 자리를 대체/확장할 예정이라, 캐싱/재시도/타임아웃 같은 실제
 * 서빙 관심사는 전혀 다루지 않는다 — 딱 "Redis GET 한 번"만 한다.
 */
@RestController
public class FeatureQueryController {

    private final StringRedisTemplate redisTemplate;
    private final String keyPrefix;

    public FeatureQueryController(
            StringRedisTemplate redisTemplate,
            @Value("${fds.feature-store.key-prefix}") String keyPrefix) {
        this.redisTemplate = redisTemplate;
        this.keyPrefix = keyPrefix;
    }

    @GetMapping(value = "/api/features/{accountId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getFeature(@PathVariable String accountId) {
        String featureJson = redisTemplate.opsForValue().get(keyPrefix + accountId);
        if (featureJson == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(featureJson);
    }
}
