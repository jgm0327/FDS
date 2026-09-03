# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-03, backend/kafka-streams-topology, session-01

## 이번 세션에서 다룬 질문/요청

- "CP2 착수하자" — CP1(계좌ID 파티셔닝) 완료 후 CP2(Kafka Streams 슬라이딩 윈도우 집계) 시작.
- 시작 전 "CP2를 어디서 분기할지" 사용자에게 확인 → **PR #1(CP1)을 먼저 main에 merge하고 나서 새
  worktree/브랜치로 분기**하는 쪽을 선택.

## 변경/결정된 내용

- **PR #1 merge**: `backend/kafka-partitioning` → `main` (`b48ce69`). main worktree(`fds`)도 fast-forward.
- **새 worktree 생성**: `C:\Users\user\Desktop\toyproject\fds-backend-streams`, 브랜치
  `backend/kafka-streams-topology`, 최신 main에서 분기. (분기 직후 upstream이 실수로 `origin/main`으로
  잡혀서 `git branch --unset-upstream`으로 정정 — 안 그러면 `git push`가 main으로 나갈 뻔했음.)
- **CP2 구현** (`com.fdsv2.sequence` 패키지 신설):
  - `AccountFeatureVector` — 출력 레코드 (accountId, recent5MinCount, amountRatio, lastTxGapSec, countryChanged)
  - `AccountActivityState` — State Store(RocksDB) 값 객체
  - `AccountActivityProcessor` — `FixedKeyProcessor` 구현체, 4개 지표 계산 로직
  - `SequenceAggregationTopologyConfig` — `@EnableKafkaStreams` 배선, `account-feature-updates` 출력 토픽 정의
  - `TransactionEventConsumer` 삭제 (BACKEND.md에 명시된 CP2 교체 대상)
  - `build.gradle`: `kafka-streams`, `kafka-streams-test-utils` 추가 (버전은 Spring Boot BOM에 위임 → 4.2.1로 resolve)
  - `application.yml`: `spring.kafka.streams.*`, `fds.kafka.feature-updates.*`, `fds.sequence-aggregation.recent-window-minutes` 추가, 죽은 `spring.kafka.consumer.*` 블록 제거
  - `.gitignore`에 `/data/`(로컬 RocksDB state) 추가
- **테스트**: `AccountActivityProcessorTest` (`TopologyTestDriver`, 3개 케이스 — 순차 계산, 윈도우 트리밍, 계좌 간 격리) 전부 통과.
- **`FdsV2BackendApplicationTests` 수정**: `spring.kafka.streams.auto-startup=false` 추가 (아래 "막혔던 문제" 참고).
- **`docs/BACKEND.md`**: "2차 구현 범위 (CP2)" 섹션 추가 — 피처 벡터 계약, 컴포넌트, 설계 결정, 검증 내역.

## 설계 의도 및 트레이드오프

- **DSL 윈도우 집계 대신 Processor API + 커스텀 State Store**: 카운트 하나만 필요하면 `SlidingWindows`로
  충분하지만, 이번엔 카운트/금액배율/경과시간/국가변경 4개를 계좌별로 하나의 상태에 묶어서 계산해야
  해서 Processor API가 더 자연스럽다고 판단.
- **"평소 금액" = 전체 기간 누적 평균**: EWMA나 최근 N분 평균이 더 정교하지만, 계좌가 오래될수록
  평균이 둔감해지는 한계를 감수하고 MVP는 단순 평균으로 시작 — 다음 개선 후보로 문서에 남김.
- **출력을 새 토픽(`account-feature-updates`)으로 분리**: CP3(Redis, 별도 브랜치)와의 경계를 "토픽
  하나가 계약"인 구조로 만들어 병렬 worktree 원칙(서로 파일 안 건드리기)을 지킴.
- **`occurredAt`(이벤트 시각) 기준 계산**: Kafka 레코드 타임스탬프가 아니라 비즈니스 이벤트 시각 기준.
  같은 계좌 이벤트가 파티션 키 덕분에 순서대로 온다는 전제 — 실측 중 실제로 `lastTxGapSec=-1`이
  한 번 관측되어(k6 부하테스트로 생성된 데이터의 미세한 순서 역전) 이 가정의 한계를 실측으로 확인함.
  이번 범위에서는 다루지 않고 문서에 한계로 명시.
- **JsonSerde(classic Jackson 2) 유지**: Spring Kafka 4.0부터 deprecated-for-removal이지만, CP1의
  프로듀서/컨슈머와 스택 일관성을 위해 그대로 사용 (Plan 서브에이전트가 실측 검증). Jackson 3 전환은
  스택 전체를 한 번에 옮기는 별도 작업으로 남김.

## 막혔던 문제와 해결 방법

1. **API 정합성 불확실성**: 이 스택(Spring Boot 4.1.1/Spring Kafka 4.1.1)이 CP1에서 이미 Jackson 2/3
   충돌을 겪은 전례가 있어서, `FixedKeyProcessor`/`JsonSerde`/`@EnableKafkaStreams` 설정을 가정으로
   구현하지 않고 Plan 서브에이전트를 시켜 실제 resolve되는 버전(kafka-streams 4.2.1)으로
   `TopologyTestDriver`를 직접 돌려서 API를 검증한 뒤 구현 시작. `JsonSerde`가 실제로 deprecated임을
   미리 발견해서 코드/문서에 그 이유를 명시할 수 있었음.
2. **`FdsV2BackendApplicationTests`가 무관한 다른 프로젝트의 Kafka(`pingbell-kafka`, 포트 9092)에 실제로
   연결해서 토픽을 만들어버림**: `@EnableKafkaStreams`가 추가되면서 `@SpringBootTest`가 기본값
   (`auto-startup=true`)으로 컨텍스트 로드 시 진짜 브로커에 연결을 시도했고, `application.yml` 기본
   `bootstrap-servers`(`localhost:9092`)가 하필 로컬의 다른 프로젝트 Kafka와 겹쳤음. 실행 후
   `pingbell-kafka`에 `transaction-events`, `...-changelog` 토픽이 생성된 걸 발견 →
   `spring.kafka.streams.auto-startup=false`로 스모크 테스트가 실제 연결을 하지 않도록 고침. 이미
   생성된 토픽 삭제는 자동 승인 정책상 차단되어(다른 프로젝트 인프라 건드리는 작업), 사용자에게
   수동 정리 명령만 안내하고 직접 삭제하지 않음.
3. **재시작 검증 중 `acc-hot-001`의 대량 과거 데이터(15,000+건, CP1 k6 테스트가 남긴 것) replay가
   오래 걸림**: `fds-v2-streams-app` 컨슈머 그룹이 새로 생기면서 `transaction-events` 토픽 전체를
   처음부터(또는 중간 커밋 오프셋부터) 다시 읽음 — 의도한 동작이지만 검증 시간이 예상보다 걸림.
   대신 "재시작 후 카운트가 1로 리셋되지 않고 이전 실행에서 멈췄던 지점부터 이어진다"는 사실 자체가
   State Store/오프셋 복구의 확실한 증거라 판단해서 전체 replay 완료를 기다리지 않고 검증을 마무리함.

## 확인 방법 (실제로 수행함)

```
./gradlew test                                                     # TopologyTestDriver, 3개 테스트 통과
SERVER_PORT=18081 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 ./gradlew bootRun
# (CP1 worktree에서 이미 띄워둔 docker-compose Kafka를 그대로 재사용 — localhost:19092)
```

- `acc-cp2-test` 계좌로 3건 연속 발행 → 로그 결과가 단위 테스트 예측과 정확히 일치:
  count 1→2→3, ratio 1.0→3.0→0.25, gap null→30→60, countryChanged false→false→true.
- `account-feature-updates` 토픽을 콘솔 컨슈머로 구독해서 실제 JSON 발행 확인 (계약대로
  `accountId`/`recent5MinCount`/`amountRatio`/`lastTxGapSec`/`countryChanged` 필드).
- 앱을 죽였다 재기동 → 4번째 거래 발행 전, `acc-hot-001`의 recentCount가 이전 실행에서 멈춘 지점
  (~8300)부터 이어서 증가(13324+)하는 걸 확인 — State Store/changelog 기반 복구 실측 확인.

## 다음에 이어서 할 일

- CP2용 k6 시나리오(짧은 시간 내 다건 연속 거래) 작성 + `docs/performance-results/`에 결과 기록
- Kafka Streams 자체 지표(process-latency, changelog lag, rebalance 등)를 Micrometer/Grafana에
  연결 — CP1 스택은 이미 있으니 패널만 확장하면 됨
- PR 생성 후 CP3(`backend/redis-feature-store`) 착수 여부 논의
- (다음 개선 후보, docs/BACKEND.md에 명시) "평소 금액" 계산을 EWMA/최근 윈도우 평균으로 개선,
  occurredAt 역전 케이스 처리
