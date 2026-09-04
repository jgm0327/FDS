package com.fdsv2.featurestore;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
 *
 * 코드 리뷰 지적: 인증/인가도 없이 계좌ID만 알면 그 계좌의 사기 탐지 피처(최근 거래 횟수, 금액
 * 배율, 국가 변경 여부 등)를 누구나 조회할 수 있어서, CP4가 늦어지면 이 "측정 전용" 엔드포인트가
 * 인증 없는 프로덕션 API로 영구히 남을 위험이 있다. 그래서 기본값 false인 플래그
 * (fds.feature-store.query-endpoint-enabled) 뒤에 숨겨서, 측정할 때만 명시적으로 켜야 하고
 * 아무 설정 없이 배포하면 이 엔드포인트 자체가 빈에 등록되지 않는다.
 */
@RestController
@ConditionalOnProperty(
        prefix = "fds.feature-store",
        name = "query-endpoint-enabled",
        havingValue = "true",
        matchIfMissing = false)
public class FeatureQueryController {

    private final StringRedisTemplate redisTemplate;
    private final FeatureStoreKeyBuilder keyBuilder;

    public FeatureQueryController(StringRedisTemplate redisTemplate, FeatureStoreKeyBuilder keyBuilder) {
        this.redisTemplate = redisTemplate;
        this.keyBuilder = keyBuilder;
    }

    @GetMapping(value = "/api/features/{accountId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getFeature(@PathVariable String accountId) {
        String featureJson = redisTemplate.opsForValue().get(keyBuilder.key(accountId));
        if (featureJson == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(featureJson);
    }
}
