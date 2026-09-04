package com.fdsv2.featurestore;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Redis 키 조립 로직을 한 곳에 모아둔다 (docs/ARCHITECTURE.md 3번 — feature:account:{accountId}).
 *
 * 코드 리뷰 지적: 이 로직이 {@link AccountFeatureStoreSinkListener}(쓰기)와
 * {@link FeatureQueryController}(읽기, 측정 전용) 두 곳에 똑같이 복붙돼 있었다 — 키 형식이
 * 바뀔 때(예: 구분자 추가) 한쪽만 고치면 조용히 서로 다른 키를 가리키게 되는 위험이 있었다.
 */
@Component
public class FeatureStoreKeyBuilder {

    private final String keyPrefix;

    public FeatureStoreKeyBuilder(@Value("${fds.feature-store.key-prefix}") String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public String key(String accountId) {
        return keyPrefix + accountId;
    }
}
