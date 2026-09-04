# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, backend/redis-feature-store, session-01

## 이번 세션에서 다룬 질문/요청

- "순서대로 계속 해줘" — PR #4(CP2 패널 확장) 리뷰·머지 이후, 마지막 순서인 CP3(Redis 피처
  스토어) 착수.
- CP3는 `docs/ARCHITECTURE.md` 3단계(온라인 피처 스토어) 스펙이 키/값/TTL까지 이미 명확히 정의돼
  있고, 기술 선택(`@KafkaListener` + Spring Data Redis)도 표준적이라, CP2 때처럼 별도 서브에이전트
  검증 없이 바로 API를 직접 확인(build.gradle 의존성 추가 후 jar 바이트코드 확인)한 뒤 구현했다.

## 변경/결정된 내용

- `build.gradle`: `spring-boot-starter-data-redis` 추가 (Lettuce 클라이언트, `StringRedisTemplate`
  자동 구성 확인함 — jar 바이트코드로 `DataRedisAutoConfiguration.stringRedisTemplate(...)` 빈
  직접 확인).
- `application.yml`: `spring.data.redis.host/port`, `spring.kafka.consumer.*`(CP3 싱크 컨슈머 전용
  — key/value 모두 String), `fds.feature-store.key-prefix`/`ttl-minutes` 추가.
- `com.fdsv2.featurestore.AccountFeatureStoreSinkListener` 신규 — `account-feature-updates` 토픽을
  구독해서 Redis에 `SET key value EX ttl`.
- `docker-compose.yml`: `redis:7-alpine` 서비스 추가 (호스트 포트 16379).
- `AccountFeatureStoreSinkListenerTest` — Mockito로 `StringRedisTemplate` 모킹, 3개 케이스.
- `docs/BACKEND.md`에 "3차 구현 범위 (CP3)" 섹션 추가.

## 설계 의도 및 트레이드오프

- **CP2의 `AccountFeatureVector` 자바 타입에 의존하지 않음**: 값을 역직렬화하지 않고 Kafka
  레코드의 원문 JSON 문자열을 그대로 Redis에 저장한다. CP2/CP3 사이의 계약을 "토픽에 담긴 JSON
  형태"로만 한정해서, CP2가 필드를 바꿔도 CP3는 재컴파일 없이 통과시킨다 — 병렬 worktree 원칙을
  코드 레벨까지 지킨 선택.
- **Kafka Streams가 아니라 평범한 `@KafkaListener`**: 이 컴포넌트는 상태 없는 "읽어서 그대로
  쓰기"라 스테이트풀 처리 엔진이 필요 없다. CP1의 (이제는 삭제된) 임시 컨슈머와 같은 도구를 쓰되,
  이번엔 정식 컴포넌트로 자리잡는다.
- **읽기(GET) API는 안 만듦**: ARCHITECTURE.md 파이프라인 경계상 "Redis 조회"는 CP4(모델 서빙)의
  역할이다. CP3는 정확히 쓰기 경로까지만 책임지고, 검증은 `redis-cli GET`으로 직접 했다.
- **Redis Exporter/Grafana 패널은 이번 범위 밖**: CP1/CP2와 같은 패턴으로 나중에 붙일 수 있는
  후속 작업으로 남겼다 — 이번 세션은 "정확히 쓰이는가"만 검증.

## 막혔던 문제와 해결 방법

- **Docker Desktop이 알 수 없는 사이 15시간 동안 죽어 있었음**: 세션 도중(아마 컴퓨터 절전) Docker
  자체가 내려갔고, 재시작 후 보니 `fds-v2-kafka`/`prometheus`/`grafana`/`kafka-exporter` 컨테이너가
  (정지가 아니라) 아예 사라져 있었다(Docker Desktop의 내부 상태 리셋으로 추정). `docker compose up -d`
  로 CP1 스택을 처음부터 재생성해서 해결 — RocksDB state/Prometheus 히스토리는 날아갔지만 이번
  검증에는 영향 없음(토픽은 앱의 `NewTopic` 빈이 재기동 시 자동 재생성).

## 확인 방법 (실제로 수행함)

```
./gradlew test                                   # 신규 3개 테스트 포함 전체 통과
docker compose up -d redis                       # (+ 위 문제로 CP1 스택도 재생성)
SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 \
  FDS_REDIS_PORT=16379 ./gradlew bootRun
```

- `acc-cp3-test` 계좌로 거래 발행 → `redis-cli GET feature:account:acc-cp3-test`로
  `{"accountId":"acc-cp3-test","recentWindowCount":1,"amountRatio":1.0,"lastTxGapSec":null,"countryChanged":false}`
  확인, `TTL` 1781초(~30분) 확인.
- 같은 계좌로 두 번째 거래 발행 → 값이 **덮어써짐**(`recentWindowCount` 1→2, `countryChanged`
  false→true, `lastTxGapSec` null→30) 확인, TTL도 1796초로 리셋 확인 — ARCHITECTURE.md의
  "새 거래마다 덮어씀 + TTL 리셋" 스펙과 실측이 정확히 일치.

## 다음에 이어서 할 일

- CP3 패널 확장(Redis Exporter — GET/SET latency, 캐시 히트율, TTL 만료 건수, 메모리 사용량;
  `docs/PERFORMANCE_MEASUREMENT.md` CP3 표 참고), CP3용 k6 시나리오(동시 다발 조회, p99 5ms 목표) —
  CP1/CP2와 같은 패턴으로 이어갈 수 있음.
- PR 리뷰(`/code-review`) 후 머지.
- CP4(PyTorch 모델 서빙) 착수 여부 논의.
