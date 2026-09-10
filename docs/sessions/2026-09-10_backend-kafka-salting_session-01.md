# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-10, backend/kafka-salting, session-01

## 이번 세션에서 다룬 질문/요청

- 자소서 초안을 검토하던 중, "핫 파티션 문제를 찾아냈다"까지만 있고 실제로 어떻게 해결했는지
  (파티션 수를 늘렸는지, 컨슈머를 스케일아웃했는지 등) 기록이 없다는 지적이 나옴.
- "이 내용에 대해서 해결할 수 있는 작업을 먼저 할 수 있나" — 자소서가 완결된 스토리(찾음 →
  해결함)를 갖도록, ARCHITECTURE.md에 처음부터 예정돼 있던 **Salting**을 실제로 구현해달라는 요청.
- 해결 범위를 사용자에게 먼저 확인: "컨슈머 스케일아웃은 이 문제에 안 먹힌다(파티션 1개는 스레드
  1개만 처리 가능)"는 걸 설명하고, Salting 정식 구현으로 진행하기로 결정.

## 변경/결정된 내용

`docs/BACKEND.md`("CP1 확장 — Salting") 섹션에 구현 스펙을 정리했다. 요약:

- `AccountShardKey`(CP1/CP2 공유 계약), `TransactionEventProducer`(고빈도 계좌만 샤드 분산),
  CP2 2단계 파이프라인(`ShardedAccountActivityProcessor` → repartition →
  `AccountActivityMergeProcessor`).
- 옛 `AccountActivityProcessor`/`AccountActivityState`는 완전히 대체(삭제)하고 새 2단계
  파이프라인으로 통합 — 일반 계좌(샤드 1개 고정)는 옛 로직과 수학적으로 동일한 결과를 내도록
  설계, 옛 테스트가 무변경으로 통과하는 것으로 검증.
- `docs/ARCHITECTURE.md` TODO 갱신: "Salting 적용 시 재집계 로직 상세 설계" 체크 완료로 표시,
  "대응 2(컨슈머 스케일아웃)"이 왜 안 먹히는지 실측 근거와 함께 명시.

## 설계 의도 및 트레이드오프

- **일반 계좌/고빈도 계좌를 하나의 코드 경로로 통일**: 처음엔 "일반 계좌는 옛 로직 그대로 두고
  고빈도 계좌만 새 파이프라인" 식으로 분기할까 고민했지만, 두 경로를 유지보수하는 비용이 크고
  버그가 한쪽에만 생겨도 발견하기 어렵다. 샤드 수를 1로 두면 새 파이프라인이 옛 로직과 수학적으로
  같아지도록 설계해서, 코드 경로를 하나로 통일하면서도 일반 계좌의 정확성을 전혀 희생하지 않았다.
- **근사값이 되는 필드(gap/countryChanged)를 명시적으로 문서화하고 방치하지 않음**: "완벽한 전역
  순서"를 salting과 동시에 지키는 건 워터마크 기반 재정렬 같은 훨씬 큰 작업이 필요해서 이번
  범위에서 하지 않았다 — 대신 "정확한 것(합/평균)"과 "근사인 것(순서 의존적 필드)"을 코드
  주석/문서 양쪽에 명확히 구분해서 남겼다. 이 프로젝트가 "순서/정합성"을 핵심 가치로 내세우는
  만큼, 트레이드오프를 숨기지 않는 게 중요하다고 판단했다.
- **샤드 수 기본값 8**: CP1이 이미 파티션 32개를 확보해뒀으니(넉넉하게 시작 원칙), 그 안에서
  감당 가능한 값으로 잡았다. 필요하면 env var로 코드 수정 없이 조정 가능.

## 막혔던 문제와 해결 방법

- 없음 — 설계를 먼저 충분히 정리(Stage 1/2 역할 분리, "이전 스냅숏 vs 이번 거래 반영" 순서)하고
  들어가서, 구현 자체는 컴파일/테스트 모두 첫 시도에 통과했다. 유일하게 조심한 부분은 Kafka
  Streams DSL이 `selectKey` 이후 Processor API 앞에서 자동으로 재파티션을 안 해준다는 것 — 미리
  알고 있던 사실이라 `repartition()`을 명시적으로 넣어서 문제 자체가 발생하지 않았다.

## 확인 방법 (실제로 수행함)

```
./gradlew test   # 옛 테스트 7건 무변경 통과 + 신규 테스트(4+3+2건) 전체 통과, 총 63개 테스트

docker compose up -d
# BEFORE: salting 없이
SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 \
  FDS_REDIS_PORT=16379 ./gradlew bootRun
k6 run -e SCENARIO=hot -e HOT_ACCOUNT_ID=acc-hot-salt k6/cp1-hot-partition-test.js

# AFTER: salting 적용
FDS_KAFKA_SALTING_HIGH_TRAFFIC_ACCOUNT_IDS=acc-hot-salted FDS_KAFKA_SALTING_SHARD_COUNT=8 \
  SERVER_PORT=18080 ... ./gradlew bootRun
k6 run -e SCENARIO=hot -e HOT_ACCOUNT_ID=acc-hot-salted k6/cp1-hot-partition-test.js
```

- **적용 전**: 15,324건 전부 파티션 1개로 집중, 컨슈머 랙 최대 **10,348건**(실제 Kafka Streams
  엔진 기준 최초 측정 — CP1 때 로그만 찍던 임시 컨슈머로는 이 랙 자체가 안 보였음).
- **적용 후**: 15,395건이 6개 파티션으로 분산, 파티션별 최대 랙 **857건**(약 12배 개선).
- Redis 최종 값의 `recentWindowCount`가 실제 발행 건수(15395)와 정확히 일치 — 재집계 정합성
  확인. `lastTxGapSec=-1` 근사 오차 1건 관측(문서화한 트레이드오프의 실제 사례).
- 검증 후 앱 종료, `docker compose down`.

## 다음에 이어서 할 일

- 코드 리뷰 후 머지.
- 자소서 초안의 "핫 파티션" 스토리를 이 결과(찾음 → 해결함, 정량 수치 포함)로 갱신.
- 재정렬(워터마크 기반)까지 필요한지는 실제 운영 데이터로 gap/countryChanged 오차 빈도를
  관측한 뒤 재검토.
- 앙상블 가중치/임계값 산정, 재학습 피드백 루프 등 ARCHITECTURE.md 나머지 TODO는 여전히 유효.
