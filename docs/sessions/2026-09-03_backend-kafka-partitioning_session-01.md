# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-03, backend/kafka-partitioning, session-01

## 이번 세션에서 다룬 질문/요청

- "fds프로젝트 진행하자" — BACKEND.md의 1차 구현 범위(Kafka 거래 이벤트 파티셔닝 검증)를 시작해달라는 요청
- 병렬 진행 범위 확인: backend/kafka-partitioning 단독 진행으로 결정 (ai/pytorch-sequence-model은 이번 세션 제외)
- 작업 도중 "왜 fds 폴더 밖(../fds-backend-kafka)에서 작업하냐"는 질문 → git worktree 특성(브랜치당 폴더 분리 필요) 설명, 같은 origin(github.com/jgm0327/FDS)에 push된다는 점 확인 후 문서(WORKTREE_SETUP.md) 방식 그대로 유지하기로 결정
- PERFORMANCE_MEASUREMENT.md가 세션 도중 갱신됨(Grafana 스냅샷 자동 저장 섹션 추가) → 참고해서 CP1 관련 구현에 반영 요청

## 변경/결정된 내용

- **Spring Boot 프로젝트 스캐폴드 생성**: `start.spring.io`로 Gradle 프로젝트 생성. 이 환경의 날짜(2026-09-03) 기준 Spring Boot 기본 버전이 4.1.1이라 그대로 사용 (그룹 `com.fdsv2`, 아티팩트 `fds-v2-backend`, Java 17 toolchain).
- **`TransactionEvent`** (`com.fdsv2.transaction`): BACKEND.md 스펙대로 record 타입, 필드 전체 반영.
- **`KafkaTopicConfig`**: `transaction-events` 토픽을 `NewTopic` 빈으로 정의. 파티션 수(32)/복제 계수(3)는 `application.yml`의 `fds.kafka.transaction-events.*`로 외부화하고, 각각 env var(`FDS_KAFKA_PARTITIONS`, `FDS_KAFKA_REPLICATION_FACTOR`)로도 오버라이드 가능하게 함 — 코드 수정 없이 조정 가능해야 한다는 BACKEND.md 요구사항을 로컬 단일 브로커 검증 상황(복제 계수 1 필요)까지 고려해서 한 단계 더 유연하게 만듦.
- **`TransactionEventProducer`**: `KafkaTemplate<String, Object>`로 발행, key=accountId. 발행 성공 시 partition/offset, 실패 시 에러를 로그로 남김. 프로듀서 설정(`enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection=5`)은 `application.yml`에 배치.
- **`TransactionEventConsumer`**: `@KafkaListener` 기반 임시 컨슈머. 클래스 주석에 "임시 컴포넌트, CP2 Kafka Streams 토폴로지로 교체 예정"임을 명시.
- **`TransactionController`**: `POST /api/transactions` — k6 테스트 진입점.
- **`application.yml`**: `application.properties`를 대체. Kafka producer/consumer 설정, `fds.kafka.*`, `fds.model-serving.torchserve.*`(base-url, timeout-ms — 값만 미리 배치, 클라이언트 구현은 다음 세션) 포함.
- **actuator + micrometer-registry-prometheus 의존성 추가**: 갱신된 `PERFORMANCE_MEASUREMENT.md`의 CP1 표("프로듀서 발행 TPS/latency → Prometheus")를 참고해서, `/actuator/prometheus`로 Kafka 프로듀서 지표를 바로 수집할 수 있도록 미리 노출. BACKEND.md 1차 범위에 명시된 항목은 아니지만, CP1 측정 자체가 이 앱의 지표에 의존하므로 지금 넣는 게 자연스럽다고 판단.
- **`docker-compose.yml`**: 로컬 검증용 단일 브로커 Kafka(KRaft 모드). 복제 계수 기본값(3)은 다중 브로커 운영 기준이라, 이 compose로 검증할 때는 `FDS_KAFKA_REPLICATION_FACTOR=1`로 오버라이드하도록 파일 상단에 주석 명시.
- **저장소 하네스 문서 커밋 (범위 외 발견 사항)**: `CLAUDE.md`, `docs/`, `.claude/settings.json`이 지금까지 git에 전혀 커밋되지 않은 untracked 상태였음을 발견. worktree는 커밋된 트리만 체크아웃하므로 이 상태로는 새 worktree에 설계 문서가 전혀 따라오지 않았음. 사용자 확인 후 `main` 브랜치에 커밋 + push하고, 이 브랜치는 rebase로 반영.

## 설계 의도 및 트레이드오프

- **Spring Boot 버전 고정 안 함**: `start.spring.io`에 특정 `bootVersion`을 명시하면 BOM 해석 오류(500)가 발생했음 (아마도 이니셜라이저 서비스 쪽 캐시/미러 이슈). 버전 파라미터를 생략하고 서비스 기본값(4.1.1)을 그대로 받는 방식으로 우회. 최신 안정 버전을 그대로 쓰는 것이 지금 시점에서는 더 안전하다고 판단.
- **파티션/복제계수 이중 오버라이드(yml 값 + env var)**: BACKEND.md는 "코드 수정 없이 조정 가능"만 요구했지만, 로컬 개발(단일 브로커, RF=1 필요)과 운영(RF=3)이 동시에 존재하는 상황을 고려해 env var 오버라이드까지 추가. 트레이드오프: yml에 플레이스홀더가 하나 더 생겨 약간 읽기 복잡해지지만, 운영 설정 파일을 건드리지 않고 로컬 검증이 가능해지는 이득이 더 크다고 판단.
- **actuator/prometheus를 1차 범위에 포함**: BACKEND.md 문서상 명시적 요구는 아니었으나, 세션 중 갱신된 PERFORMANCE_MEASUREMENT.md의 CP1 지표 표가 이 앱의 Prometheus 노출에 직접 의존하는 구조라 지금 넣지 않으면 CP1 측정 자체가 막힘. 범위를 살짝 넘지만 "다음 세션에서 다시 넣어야 할 것"보다는 지금 넣는 게 더 자연스럽다고 판단.
- **ErrorHandlingDeserializer 미적용**: 임시 컨슈머는 어차피 CP2에서 통째로 교체될 컴포넌트라, 역직렬화 실패 방어 로직까지 넣는 건 과한 투자라고 판단해 생략. (다음에 진짜 Streams 토폴로지로 갈 때 정식으로 고려)

## 막혔던 문제와 해결 방법

- **`start.spring.io`에 `bootVersion=4.1.1.RELEASE`/`4.0.8.RELEASE`를 명시하면 500 에러**: "Bom ... could not be resolved". `bootVersion` 파라미터를 아예 빼고 요청하니 정상 동작(서비스가 내부적으로 캐시된 기본 버전은 문제없이 resolve). 원인까지는 특정하지 못했고, 우회로 해결.
- **worktree가 `../fds-backend-kafka`로 분리되는 것에 대한 사용자 혼란**: git worktree의 "브랜치당 폴더 분리 필요" 특성과 "같은 `.git`/origin을 공유하므로 push는 같은 저장소로 간다"는 점을 설명해서 해소.
- **`docs/`, `CLAUDE.md`가 worktree에 안 보임**: 원인 추적 결과 애초에 main 브랜치에 커밋된 적 없는 untracked 파일이었음. `git worktree add`는 커밋된 트리만 체크아웃하므로 발생한 문제. main에 커밋 + push 후 이 브랜치를 rebase해서 해결.
- **Docker Desktop이 꺼져 있어 실제 브로커 기동 실증은 못 함**: `docker compose up -d` 시 `dockerDesktopLinuxEngine` 파이프 연결 실패. 컴파일(`./gradlew compileJava`)과 단위 테스트(`./gradlew test`, Kafka 브로커 없이도 컨텍스트 로딩은 성공)까지는 확인함. 실제 브로커로 "같은 accountId → 같은 partition" 검증은 다음으로 미룸.

## 다음에 이어서 할 일

- Docker Desktop을 켜고 `docker compose up -d` → 같은 accountId로 `POST /api/transactions` 여러 번 호출 → 컨슈머 로그에서 partition 번호가 항상 같은지 실제로 확인 (BACKEND.md "완료 후 확인 방법")
- CP1 k6 시나리오(핫 파티션 재현) 작성 및 실행, `docs/PERFORMANCE_MEASUREMENT.md`의 Grafana 스냅샷 저장 규칙에 맞춰 `docs/performance-results/`에 결과 기록 (아직 이 디렉토리 자체가 없음 — 별도로 만들어야 함)
- PR 생성 (`backend/kafka-partitioning` → `main`)
- 이후 세션: `backend/kafka-streams-topology` (CP2), `ai/pytorch-sequence-model` 등 병렬 트랙 착수 여부 논의
