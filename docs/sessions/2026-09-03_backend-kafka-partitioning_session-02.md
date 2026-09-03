# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-03, backend/kafka-partitioning, session-02

## 이번 세션에서 다룬 질문/요청

- "docker desktop켰으니까 못했던 부분 다시 진행해줘" — session-01에서 Docker Desktop이 꺼져 있어 못 했던 실제 브로커 기동 + 파티셔닝 실증 검증을 이어서 진행

## 변경/결정된 내용

- **`docker-compose.yml` 호스트 포트를 19092로 변경**: 로컬에 이미 다른 프로젝트(`pingbell-kafka`)가 9092를 점유 중이라 충돌 발생 → 호스트 포트만 19092로 바꾸고 주석에 이유와 실행 예시 커맨드 추가.
- **`build.gradle`에 classic Jackson 2 의존성 2개 추가**:
  - `com.fasterxml.jackson.core:jackson-databind:2.21.6`
  - `com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.21.6`
  - 원인: Spring Boot 4.1.1은 기본적으로 Jackson 3(`tools.jackson.*`)을 쓰지만, Spring Kafka 4.1.1의 `JsonSerializer`/`JsonDeserializer`는 아직 classic Jackson 2(`com.fasterxml.jackson.databind`) API 기반이라 별도로 클래스패스에 올려야 함. 추가로 `TransactionEvent.occurredAt`이 `java.time.Instant`라 `jackson-datatype-jsr310` 모듈 없이는 직렬화가 실패함.
- **실제 브로커로 end-to-end 파티셔닝 검증 완료** (아래 "확인 방법" 참고).

## 설계 의도 및 트레이드오프

- **호스트 포트를 코드 변경 없이 env var로 우회하지 않고 compose 파일 자체를 수정**: `KAFKA_BOOTSTRAP_SERVERS` 앱 설정은 이미 env var로 열려 있었지만, docker-compose 쪽 호스트 포트 바인딩(`9092:9092`)은 로컬 환경마다 충돌 여지가 있는 값이라 아예 다른 프로젝트와 겹치지 않는 포트(19092)로 고정하는 게 재현성 측면에서 더 낫다고 판단.
- **Jackson 2/3 공존을 그대로 둠**: Spring Boot 4가 기본으로 미는 Jackson 3(REST 직렬화 등에 사용)와, Kafka 메시지 직렬화에만 쓰는 classic Jackson 2가 같은 클래스패스에 공존하게 됨. 두 모듈이 서로 다른 패키지(`tools.jackson.*` vs `com.fasterxml.jackson.*`)라 충돌은 없지만, 장기적으로는 Spring Kafka가 Jackson 3를 지원하는 버전으로 올라오면 이 별도 의존성을 제거할 수 있을 것 — 다음에 Spring Kafka 버전을 올릴 때 재검토 필요.

## 막혔던 문제와 해결 방법

1. **`docker compose up -d` 시 "port is already allocated" (9092)**: 로컬에 무관한 다른 프로젝트의 `pingbell-kafka` 컨테이너가 이미 9092를 쓰고 있었음. 그 컨테이너는 건드리지 않고, 이 프로젝트의 compose 호스트 포트를 19092로 변경해서 해결.
2. **`bootRun` 시 "Port 8080 was already in use"**: 마찬가지로 다른 프로젝트(`pingbell-app`)가 8080을 점유 중. `SERVER_PORT=18080` env var로 우회 (코드/설정 파일 변경 없이).
3. **컨슈머 기동 시 `NoClassDefFoundError: com.fasterxml.jackson.databind.JavaType`**: Spring Boot 4의 기본 Jackson 3 스택에는 classic Jackson 2 databind가 없음. `jackson-databind:2.21.6`을 명시적으로 추가해서 해결 (버전은 이미 transitively 들어와 있던 `jackson-annotations:2.21`과 맞춰 선택).
4. **`POST /api/transactions` 호출 시 500, 원인은 `InvalidDefinitionException: Java 8 date/time type Instant not supported`**: `jackson-datatype-jsr310` 모듈이 클래스패스에 없어서 발생. 의존성 추가로 해결.

## 확인 방법 (실제로 수행함)

```
docker compose up -d   # 단일 브로커 Kafka (호스트 포트 19092)
SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 ./gradlew bootRun
```

- `transaction-events` 토픽이 32개 파티션으로 정상 생성됨 (컨슈머 로그에 `transaction-events-0` ~ `transaction-events-31` 전체 assign 확인).
- `acc-001` 계좌로 5건 연속 발행 → 프로듀서/컨슈머 로그 모두 **partition=22로 5건 전부 일치**.
- `acc-002` 계좌로 3건 발행 → **partition=11로 3건 전부 일치**.
- `acc-003` 계좌로 3건 발행 → **partition=27로 3건 전부 일치**.
- 계좌마다 서로 다른 파티션(22/11/27)에 분산됨 — "같은 계좌 → 항상 같은 파티션, 다른 계좌 → 분산" 이라는 파티셔닝 설계 목표를 실측으로 확인.
- `/actuator/prometheus`에서 `kafka_producer_batch_size_avg` 등 프로듀서 지표가 정상 노출되는 것도 확인 (CP1 측정 파이프라인 연동 확인).

## 다음에 이어서 할 일

- CP1 k6 시나리오(핫 파티션 재현) 작성 및 실행, `docs/PERFORMANCE_MEASUREMENT.md`의 Grafana 스냅샷 저장 규칙에 맞춰 `docs/performance-results/`에 결과 기록
- Spring Kafka가 Jackson 3를 네이티브 지원하는 버전으로 올라오면 classic Jackson 2 의존성 제거 검토
- `backend/kafka-streams-topology`(CP2) 세션 착수
