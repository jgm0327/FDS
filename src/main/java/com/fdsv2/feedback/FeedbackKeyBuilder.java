package com.fdsv2.feedback;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * CP6 Redis 키 조립 로직을 한 곳에 모아둔다 — {@code com.fdsv2.featurestore.FeatureStoreKeyBuilder}가
 * "쓰기/읽기 두 곳에 키 형식이 복붙돼 있으면 한쪽만 바뀌었을 때 조용히 어긋난다"는 문제를 이미
 * 겪었던 것과 같은 이유로, CP6도 처음부터 키 조립을 이 클래스 하나로 모은다.
 *
 * <p>pending 레코드 하나당 키 하나({@code feedback:pending:{decisionId}}) + 만기 조회용 정렬집합
 * 인덱스({@code feedback:pending:index}, score=라벨 공개 예정 시각의 epoch초) 조합이다 —
 * {@code KEYS}로 전체 스캔하지 않고 {@code ZRANGEBYSCORE}로 "지금 공개해야 할" 항목만 뽑기 위함
 * ({@link FeedbackLabelScheduler} 참고).
 */
@Component
public class FeedbackKeyBuilder {

    private final String pendingKeyPrefix;
    private final String pendingIndexKey;

    public FeedbackKeyBuilder(
            @Value("${fds.feedback.pending-key-prefix:feedback:pending:}") String pendingKeyPrefix,
            @Value("${fds.feedback.pending-index-key:feedback:pending:index}") String pendingIndexKey) {
        this.pendingKeyPrefix = pendingKeyPrefix;
        this.pendingIndexKey = pendingIndexKey;
    }

    public String pendingKey(String decisionId) {
        return pendingKeyPrefix + decisionId;
    }

    public String pendingIndexKey() {
        return pendingIndexKey;
    }
}
