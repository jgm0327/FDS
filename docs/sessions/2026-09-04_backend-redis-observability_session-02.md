# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, backend/redis-observability, session-02

## 이번 세션에서 다룬 질문/요청

- PR #6(CP3 패널 확장)을 `/code-review`로 리뷰 후 반영.

## 변경/결정된 내용

- `/code-review`로 PR #6을 medium 강도로 리뷰 — 5개 항목 발견, 4개 반영 + 1개는 실측으로 반박:
  1. **quantile 라벨 불일치 지적은 실측으로 반박됨**: 리뷰가 "quantile 값이 '50'이지 '50.0'이
     아니다"라고 지적했는데, 이 세션에서 실제로 두 번 다른 값을 관측했다(한 번은 "50.0", 재확인
     때는 "50" — 같은 exporter, 같은 실행 방식인데도 비결정적). 리뷰의 정적 분석 결과와 내
     실측 결과가 둘 다 부분적으로 맞았던 것 — 그래서 어느 한쪽으로 고정하지 않고 정규식
     (`quantile=~"50(\.0)?"`)으로 둘 다 매칭하도록 고쳤다. Grafana를 통해 실제 렌더링까지
     확인해서 정규식이 제대로 동작하는 것도 검증함.
  2. **GET 엔드포인트가 인증 없이 영구히 노출될 위험**: `fds.feature-store.query-endpoint-enabled`
     플래그(기본 false)로 게이팅 — `@ConditionalOnProperty`로 켜지 않으면 빈 자체가 등록 안 됨.
  3. **k6 warm_up→lookups 타이밍이 레이스 컨디션**: `setup()` 함수에서 워밍업 후 실제로 Redis에
     반영됐는지 폴링(최대 20초)하도록 재작성 — 고정 5초 대기보다 훨씬 견고함.
  4. **prometheus의 depends_on에 redis-exporter 누락**: kafka-exporter와 같은 패턴으로 추가.
  5. **Redis 키 조립 로직이 두 클래스에 중복**: `FeatureStoreKeyBuilder` 컴포넌트로 추출해서
     `AccountFeatureStoreSinkListener`(쓰기)와 `FeatureQueryController`(읽기)가 공유.

## 설계 의도 및 트레이드오프

- **리뷰 결과를 무비판적으로 따르지 않고 실측으로 재검증함**: quantile 라벨 건은 리뷰가 소스 코드
  분석/공개 예시 기반으로 판단한 것이었는데, 실제로 컨테이너를 다시 띄워서 확인해보니 리뷰의
  주장과 내 이전 관측이 둘 다 한 번씩 나온 비결정적 현상이었다. 리뷰 지적을 그대로 받아 적용
  ("50"으로 고정)했다면 오히려 다시 깨질 수 있었던 상황 — 실측으로 근본 원인(비결정성)을 파악하고
  정규식으로 두 경우 다 커버하는 게 올바른 수정이라고 판단했다.
- **`query-endpoint-enabled` 기본값 false**: 이 프로젝트에 아직 별도 운영/개발 프로파일 체계가
  없어서, `@Profile` 대신 명시적 env var 플래그로 껐다 — CP1부터 이어온 "코드 수정 없이 조정
  가능하게" 원칙과 일관됨.

## 확인 방법 (실제로 수행함)

```
./gradlew test                                                    # 전체 통과 (생성자 변경 반영)
FDS_FEATURE_STORE_TTL_MINUTES=1 FDS_FEATURE_STORE_QUERY_ENDPOINT_ENABLED=true \
  SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 \
  FDS_REDIS_PORT=16379 ./gradlew bootRun
k6 run k6/cp3-feature-lookup-test.js
```

- k6 재실행 후 `http_req_failed`가 9.84%로 의도한 10%와 정확히 일치 — setup() 폴링 방식이
  레이스 컨디션을 실제로 해결했음을 확인.
- Grafana에서 정규식 quantile 쿼리로 GET/SET p50/p99 4개 라인이 전부 정상 렌더링되는 것 확인
  (스크린샷 갱신).
- `query-endpoint-enabled=true`로 켰을 때 `/api/features/{accountId}`가 정상 동작하는 것 확인
  (기본값 false일 때 빈 자체가 등록 안 되는 건 `@ConditionalOnProperty`의 표준 동작이라 별도
  검증 없이 신뢰).

## 다음에 이어서 할 일

- PR #6 push 및 머지.
- CP4(PyTorch 모델 서빙) 착수 여부 논의.
