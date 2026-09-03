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
