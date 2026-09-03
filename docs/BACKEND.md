# FDS v2 BACKEND — 구현 스펙

이 문서는 실제 코드가 아니라, **Claude Code가 이어서 구현할 때 참고할 스펙 문서**다.
설계 배경은 `docs/ARCHITECTURE.md`, 성능 측정 기준은 `docs/PERFORMANCE_MEASUREMENT.md` 참고.

## 1차 구현 범위

Kafka 프로듀서/컨슈머를 통한 거래 이벤트 파티셔닝 검증 (CP1 대응)

## 프로젝트 기본 정보

- 언어/프레임워크: Java 17, Spring Boot 3.x
- 빌드 도구: Gradle
- 메시징: Spring Kafka
- 패키지 루트: `com.fdsv2`

## 구현해야 할 컴포넌트 목록

### 1. `TransactionEvent` (도메인 모델)

- 위치: `com.fdsv2.transaction`
- 필드: `transactionId`(String), `accountId`(String), `amount`(BigDecimal), `merchantCategory`(String), `country`(String), `occurredAt`(Instant)
- record 타입으로 불변 객체로 구현

### 2. Kafka 토픽 설정

- 토픽명: `transaction-events`
- **파티션 수: 32개** (초기값 — 근거는 아래 "핵심 설계 결정" 참고)
- 복제 계수: 3
- `application.yml`에서 파티션 수/복제 계수를 외부 설정값으로 뺄 것 (코드 수정 없이 조정 가능하게)

### 3. 거래 이벤트 프로듀서

- `KafkaTemplate<String, Object>`를 사용해 `transaction-events` 토픽에 발행
- **반드시 accountId를 메시지 key로 명시** — 파티셔닝의 핵심 전제조건
- 프로듀서 설정: `enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection=5`
  - 이유: 재시도 상황에서도 파티션 내 순서가 깨지지 않도록 보장 (Kafka 공식 권장 조합)
- 발행 성공/실패 시 accountId, partition, offset을 로그로 남길 것 (파티셔닝 검증에 필요)

### 4. 파티셔닝 검증용 임시 컨슈머

- `@KafkaListener`로 `transaction-events` 구독
- 목적: "같은 accountId가 항상 같은 파티션으로 들어오는가"를 로그로 확인
- accountId, partition, offset을 로그에 출력
- **임시 컴포넌트임을 클래스 주석에 명시** — 나중에 Kafka Streams 슬라이딩 윈도우 토폴로지(CP2)로 교체될 예정이므로, 교체 시점을 코드에 남겨둘 것

### 5. k6 부하테스트용 REST 엔드포인트

- `POST /api/transactions` — `TransactionEvent`를 요청 바디로 받아 프로듀서로 발행
- 이 엔드포인트는 CP1 부하테스트(핫 파티션 재현 시나리오)의 진입점

### 6. 모델 서빙 연동 설정 (뼈대만, 실제 호출 로직은 다음 단계)

- `application.yml`에 TorchServe REST 엔드포인트 base-url, timeout 값을 미리 설정해둘 것
- 실제 `ModelInferenceClient` 인터페이스/구현체는 이번 범위에 포함하지 않음 (다음 세션)

## 핵심 설계 결정 (Claude Code가 구현 시 반영해야 할 이유)

1. **파티션 수 = 32 (초기값)**: 실측 데이터가 없는 상태에서 정밀 계산이 불가능하므로 "넉넉하게 시작 → k6 실측 후 조정" 원칙 적용. 파티션 수는 운영 중 늘리면 accountId-파티션 매핑이 깨지므로, 트래픽이 없는 지금 단계에서만 유연하게 조정 가능한 값으로 취급한다.
2. **프로듀서 신뢰성 설정**: 순서 보장이 이 시스템의 핵심 전제조건이므로 멱등성 프로듀서 설정을 기본값으로 반영한다.
3. **임시 컨슈머를 먼저 구현하는 이유**: Kafka Streams 토폴로지(상태 저장소, changelog topic 등 고려사항 많음)를 바로 만들면 파티셔닝 자체의 정상 동작 확인이 늦어진다. 최소 기능으로 먼저 검증 후 교체하는 단계적 접근을 취한다.
4. **TorchServe는 REST 먼저, gRPC는 다음 단계**: REST로 기준선(baseline) latency를 먼저 확보하고, 이후 gRPC로 교체해 k6로 성능 비교(CP4)할 계획이므로, 지금 단계는 REST 설정값만 미리 자리를 잡아둔다.

## 이번 범위에서 제외하는 것 (다음 세션으로 미룸)

- Kafka Streams 슬라이딩 윈도우 집계 토폴로지 (CP2)
- Redis 피처 스토어 연동
- `ModelInferenceClient` (TorchServe 호출 클라이언트)
- Resilience4j Circuit Breaker 설정
- 규칙 엔진 + 앙상블 판정 로직

## 완료 후 확인 방법

로컬 Kafka(예: Docker Compose)를 띄우고, `POST /api/transactions`로 같은 accountId를 여러 번 호출했을 때 컨슈머 로그에 항상 같은 partition 번호가 찍히는지 확인한다.

---

## 2차 구현 범위 (CP2) — `backend/kafka-streams-topology` 브랜치

Kafka Streams 슬라이딩 윈도우 집계 토폴로지. CP1의 임시 검증 컨슈머(`TransactionEventConsumer`)를
교체한다 (위 4번 참고). `docs/ARCHITECTURE.md` 2단계("실시간 시퀀스 집계")에 대응.

### 피처 벡터 계약 (CP3와의 인터페이스)

`AccountFeatureVector` — `account-feature-updates` 토픽(계좌ID 키)으로 발행. CP3(Redis 피처 스토어,
별도 브랜치)가 그대로 소비할 형식이므로 필드를 바꿀 때는 CP3도 함께 고려해야 한다.

```json
{"accountId": "acc-1", "recentWindowCount": 7, "amountRatio": 15.2, "lastTxGapSec": 12, "countryChanged": true}
```

(ARCHITECTURE.md 원문 예시는 필드명이 `recent_5min_count`지만, 윈도우 크기가 설정 가능해서 "5분"을
이름에 고정하면 다른 윈도우로 튜닝했을 때 의미가 어긋난다는 걸 코드 리뷰에서 지적받아
`recentWindowCount`로 바꿨다 — CP3가 아직 없어 지금 바꿔도 깨지는 소비자가 없음.)

- `recentWindowCount`: 설정 가능한 윈도우(기본 5분, `fds.sequence-aggregation.recent-window-minutes`) 내
  거래 횟수(이번 거래 포함)
- `amountRatio`: 이번 거래 금액 / "평소 금액"(이번 거래 이전까지의 전체 기간 단순 평균). 비교 대상이
  없는 첫 거래는 1.0
- `lastTxGapSec`(nullable): 직전 거래 이후 경과 시간(초). 첫 거래는 null
- `countryChanged`: 직전 거래와 국가가 다르면 true. 첫 거래는 false

### 구현 컴포넌트

- `com.fdsv2.sequence.AccountFeatureVector` — 위 계약 그대로의 출력 레코드
- `com.fdsv2.sequence.AccountActivityState` — State Store(RocksDB, 계좌ID 키) 값 객체. 최근 윈도우
  타임스탬프 목록 + 전체 기간 누적 합계/건수 + 마지막 거래 시각/국가
- `com.fdsv2.sequence.AccountActivityProcessor` — `FixedKeyProcessor` 구현체. 4개 지표 계산 + 윈도우
  트리밍
- `com.fdsv2.sequence.SequenceAggregationTopologyConfig` — `@EnableKafkaStreams` 배선. 입력
  `transaction-events` → Processor API → 출력 `account-feature-updates`

### 핵심 설계 결정

1. **DSL 윈도우 집계가 아니라 Processor API + 커스텀 State Store**: `SlidingWindows`/`TimeWindows`는
   카운트 하나는 잘 뽑지만, 4개 지표가 서로 다른 계산이라 하나의 계좌별 상태로 묶어서 계산하는 게
   자연스럽다. `KStream.processValues(FixedKeyProcessorSupplier, storeName)` + 커스텀
   `KeyValueStore<String, AccountActivityState>`(changelog 로깅 기본 활성화)를 쓴다.
2. **"평소 금액" = 전체 기간 단순 평균**: 이동평균(EWMA)이나 최근 N분 윈도우 평균이 계좌의 소비 패턴
   변화에 더 민감하지만, MVP는 전체 기간 누적 평균으로 시작한다. 계좌가 오래될수록 평균이 둔감해지는
   한계가 있음 — 다음 개선 후보.
3. **출력은 새 토픽으로 분리**: Streams 프로세스 안에서 바로 Redis에 쓰지 않고 토픽으로 발행 —
   CP3가 별도 브랜치이므로 "토픽이 계약"인 구조가 병렬 worktree 원칙에 맞다.
4. **`TransactionEvent.occurredAt()`(이벤트 발생 시각) 기준 계산**: Kafka 레코드 타임스탬프가 아니라
   비즈니스 이벤트 시각을 기준으로 슬라이딩 윈도우/경과시간을 계산한다. accountId가 파티션 키라
   같은 계좌 이벤트는 대체로 순서대로 도착하지만, 클라이언트 시계 오차나 재시도로 역전된 이벤트가
   섞일 수 있다는 걸 전제로 방어적으로 짰다 (아래 "코드 리뷰로 발견/수정한 문제" 참고).
5. **JsonSerde(classic Jackson 2) 유지, JacksonJsonSerde(Jackson 3)로 안 옮김**: Spring Kafka 4.0부터
   `JsonSerde`가 `@Deprecated(forRemoval = true)`이지만, CP1의 프로듀서/컨슈머가 이미 classic
   Jackson 2 기반이라 지금 Streams만 Jackson 3로 옮기면 두 Jackson 스택이 동시에 클래스패스에 올라가
   복잡도만 는다. 스택 전체를 한 번에 옮기는 별도 작업으로 남겨둔다.

### 코드 리뷰로 발견/수정한 문제

머지 전 `/code-review`로 발견해서 이번 PR에 바로 반영한 것들 (자세한 재현/근거는 세션 로그 참고):

- **역전된 이벤트가 상태를 영구히 오염시키는 버그**: 슬라이딩 윈도우 트리밍이 앞쪽만 보던 걸
  `removeIf`로 바꾸고, `lastTransactionTimestamp`/`lastCountry`는 "지금까지 본 것 중 가장 최근" 값만
  갱신하도록 가드를 추가했다. `TopologyTestDriver`로 실제 재현/수정 확인함(회귀 테스트 2개 추가).
- **occurredAt/amount가 null이면 NPE로 스트림 스레드 전체가 죽는 문제**: `StreamsUncaughtExceptionHandler`가
  없는 상태라 예외 하나가 전체 파이프라인(다른 계좌까지)을 멈췄다 — 프로세서에서 null 필드를
  검증해서 해당 레코드만 건너뛰도록 수정.
- **CP1 Grafana 대시보드의 컨슈머 랙 패널이 죽은 컨슈머 그룹을 쿼리하던 문제**: CP2가
  `TransactionEventConsumer`(그룹 `fds-v2-partitioning-check`)를 지우면서 그 패널이 영구히 빈
  상태가 되던 걸, Kafka Streams의 컨슈머 그룹(`fds-v2-streams-app`)을 쿼리하도록 수정.
- **`@SpringBootTest`가 `auto-startup=false`만으로는 로컬의 다른 Kafka에 토픽을 만드는 걸 못 막던
  문제**: `spring.kafka.admin.auto-create=false` + `bootstrap-servers=localhost:1`로 이중 차단.
- **테스트가 프로덕션 토폴로지 배선을 따로 베껴 써서 드리프트 위험이 있던 문제**:
  `SequenceAggregationTopologyConfig`에서 배선 로직을 static 메서드로 뽑아 테스트가 그대로 재사용.
- **핫 파티션 상황에서 레코드마다 log.info가 병목이 될 수 있던 문제**: `log.debug`로 낮춤.
- **`amountRatio`가 누적 합계가 음수(환불 등)일 때 부호가 뒤집힌 값을 내던 문제**: 0 이하 전부
  가드해서 1.0으로 처리.
- **`recent5MinCount` 필드명이 설정 가능한 윈도우와 안 맞던 문제**: `recentWindowCount`로 개명
  (위 "피처 벡터 계약" 참고).

### 이번 범위에서 제외하는 것 (의도적으로 남겨둔 것)

- **transactionId 중복 제거**: 프로듀서 멱등성(`enable.idempotence`)은 브로커 재시도 중복만 막고,
  애플리케이션 레벨 재시도(같은 transactionId로 재발행)는 여전히 이중 집계된다. 최근 N개
  transactionId를 보고 걸러내는 등의 dedup 로직은 State Store 스키마 확장이 필요한 별도 작업이라
  이번 범위에서 제외 — 다음 개선 후보.
- CP2용 k6 시나리오(짧은 시간 내 다건 연속 거래) + Grafana 패널 확장 — 다음 세션
- Redis 피처 스토어 연동 (CP3)
- RocksDB metrics exporter 등 CP2 전용 관측 스택 — CP1의 Prometheus/Grafana 스택은 이미 있으므로,
  Kafka Streams 자체 지표(process-latency 등)를 Micrometer에 바인딩하는 작업만 남았고 이번엔 하지 않음

### 완료 후 확인 방법 (실제로 수행함)

1. `./gradlew test` — `TopologyTestDriver`로 브로커 없이 로직 검증. 계좌별 카운트/비율/경과시간/
   국가변경 시퀀스, 윈도우 트리밍, 계좌 간 상태 격리, 역전 이벤트에 대한 방어 로직(경과시간 오염
   방지/카운트 영구 부풀림 방지), null 필드 방어까지 총 6개 테스트.
2. 실제 브로커(CP1 docker-compose)로 e2e 검증: 계좌 하나에 3건 연속 발행 → 단위 테스트와 정확히 같은
   결과(count 1→2→3, ratio 1.0→3.0→0.25, gap null→30→60, countryChanged false→false→true) 확인,
   `account-feature-updates` 토픽에서 콘솔 컨슈머로 실제 발행된 JSON도 확인
3. 앱을 죽였다 재기동해서 State Store 복구 확인: 재시작 후 카운트가 1로 리셋되지 않고 이전 실행에서
   멈췄던 지점부터 이어짐 — changelog/오프셋 기반 복구가 정상 동작함을 실측으로 확인
   (docs/ARCHITECTURE.md 2번 "장애 대비" 요건)
4. `/code-review`로 머지 전 리뷰 → 실질적인 버그 다수 발견 → 전부 수정 후 재테스트 통과 확인
   (위 "코드 리뷰로 발견/수정한 문제" 참고)
