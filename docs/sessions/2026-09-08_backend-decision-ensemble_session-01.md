# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-08, backend/decision-ensemble, session-01

## 이번 세션에서 다룬 질문/요청

- CP5(판정 및 대응, docs/ARCHITECTURE.md 5번) 구현. 요구사항 3가지:
  1. 규칙 엔진(하드룰 OR + 화이트리스트) + 모델 확률 앙상블
  2. 최종 액션 3단계(허용/추가인증/차단)
  3. 지금은 `GET /api/fraud-score/{accountId}`를 수동 호출해야만 판정이 동작하는데, 거래 이벤트 →
     자동 판정까지 실제로 이어지도록 트리거 배선
- 시작 전 docs/BACKEND.md, docs/sessions/ 최근 로그를 먼저 훑고 설계 방향을 잡을 것.

## 변경/결정된 내용

- **사전 조사**: docs/BACKEND.md에는 CP4(model-client) 구현 스펙이 아예 안 실려 있다 — 코드는
  main에 병합됐지만 문서화가 안 된 상태였다. 대신 `docs/sessions/2026-09-04_backend-model-client_*`,
  `..._model-client-observability_*`, `..._sequence-window-feature-store_*` 세션 로그로 실제
  설계를 재구성했다. 이 문서 갱신은 이번 범위에 포함하지 않음(CP5 자체가 아니라 CP4 문서화 누락이라
  별개 이슈로 남겨둠).
- **`com.fdsv2.decision` 패키지 신설**:
  - `Action`(enum ALLOW/STEP_UP_AUTH/BLOCK), `RuleVerdict`, `FraudDecision`(레코드)
  - `RuleEngine` — 화이트리스트(콤마 구분 accountId 목록, 모델 호출 없이 즉시 ALLOW) → 하드룰
    OR(블랙리스트 / 금액배율≥임계치 && 국가변경, 즉시 BLOCK) → 둘 다 아니면 CP4의
    `RuleBasedFallbackScorer.score()`를 그대로 재사용해 규칙 점수 산출.
  - `FraudDecisionService`(인터페이스) / `EnsembleFraudDecisionService`(구현) — 규칙 엔진이 즉시
    액션을 정하면 모델을 아예 호출하지 않고, 그렇지 않으면 `ModelInferenceClient.predict()`를
    호출해 `combinedScore = modelWeight*모델확률 + ruleWeight*규칙점수`를 계산, 임계값
    비교(low/high)로 3단계 액션 산출. 판정 결과를 구조화 로그 한 줄로 남김(피드백 루프의 현재
    범위).
  - `FraudDecisionEventListener` — 자동 트리거 본체(아래 "설계 의도" 참고).
  - `FraudDecisionController` — `GET /api/fraud-decision/{accountId}`(측정 전용, 기본 비활성).
- **`com.fdsv2.featurestore.FeatureStoreUpdatedEvent`**(신규) + `AccountFeatureStoreSinkListener`
  수정 — Redis 쓰기(SET+RPUSH+TRIM+EXPIRE)가 전부 끝난 직후 이 이벤트를 발행하도록 한 줄 추가.
- `application.yml`: `fds.decision.*`(화이트리스트/블랙리스트/하드룰 임계치/앙상블 가중치·임계값/
  측정용 엔드포인트 플래그) 추가.
- 테스트 16건 신규(`RuleEngineTest` 7, `EnsembleFraudDecisionServiceTest` 5,
  `FraudDecisionEventListenerTest` 2, `FraudDecisionControllerTest` 2) + 기존
  `AccountFeatureStoreSinkListenerTest`에 이벤트 발행 검증 1건 추가/생성자 변경 반영. 전체
  `./gradlew test` 통과.

## 설계 의도 및 트레이드오프

- **자동 트리거를 별도 Kafka 컨슈머 그룹이 아니라 Spring `ApplicationEventPublisher`로 배선함**
  (요구사항 3번의 핵심 결정). CP1~CP4는 전부 "같은 토픽을 각자 독립 컨슈머 그룹으로 구독"하는
  패턴이었다(토픽이 계약). CP5도 `account-feature-updates`를 독립 컨슈머 그룹으로 구독하는 안을
  먼저 검토했지만, 그러면 CP3의 Redis 쓰기(RPUSH)와 CP5의 읽기(`ModelInferenceClient` →
  `AccountRecentSequenceReader`가 같은 Redis LIST를 읽음) 사이에 레이스 컨디션이 생긴다 — 서로
  다른 컨슈머 그룹은 같은 레코드를 거의 동시에 받을 뿐 순서를 보장하지 않아서, CP5가 이번 거래의
  RPUSH보다 먼저 모델을 호출하면 "방금 들어온 거래가 반영 안 된" 시퀀스로 판정하게 된다. Spring
  이벤트는 별도 TaskExecutor 설정이 없으면 발행자와 같은 스레드에서 동기 호출되므로, CP3가 Redis
  쓰기를 다 마친 "이후"에 발행하면 이 레이스가 애초에 안 생긴다 — 이번엔 정확성이 "단계별 독립
  컨슈머 그룹"이라는 기존 패턴의 일관성보다 중요하다고 판단했다. 비용은 CP3 파일에 몇 줄이 추가된
  것뿐이고, 그 몇 줄은 "누가 구독하는지 몰라도 되는" 범용 확장 지점이라 CP5 로직이 바뀌어도 CP3를
  다시 건드릴 필요는 없다.
- **CP5 리스너(`FraudDecisionEventListener`)가 예외를 반드시 자기 선에서 삼킴**: 동기 이벤트라
  여기서 예외가 새면 CP3의 Kafka 리스너 스레드까지 전파되고, 이미 성공한 Redis 쓰기가 있는
  레코드가 `FeatureStoreKafkaConfig`의 `DefaultErrorHandler`에 의해 불필요하게 재시도(RPUSH
  중복)될 수 있다. `AccountFeatureStoreSinkListenerTest`에 이미 "Redis 쓰기 실패는 예외를 삼키지
  않고 전파해야 재시도된다"는 정반대 계약의 테스트가 있어서, 이 둘을 혼동하면 안 된다는 걸 코드
  작성 전에 먼저 확인했다.
- **`RuleBasedFallbackScorer`(CP4)를 삭제/흡수하지 않고 읽기 전용으로 재사용**: 그 클래스
  javadoc이 "CP5가 만들어지면 이 클래스가 CP5 규칙 엔진으로 흡수될 가능성이 높다"고 예고했었지만,
  지금 `git worktree list`로 확인해보니 `backend/model-client-concurrency-fix` 브랜치가 CP4의
  CircuitBreaker 동시성 문제(`model-client-observability` 세션 로그의 미해결 이슈)를 병렬로 고치는
  중이다. `ModelClientConfig`/`TorchServeModelInferenceClient` 등 그 브랜치가 만질 가능성이 높은
  파일은 전혀 건드리지 않고, `RuleBasedFallbackScorer.score()`만 그대로 호출해서 재사용했다 —
  이러면 그 브랜치가 나중에 병합돼도 충돌 여지가 없다.
- **앙상블 가중치 0.7(모델) : 0.3(규칙), 임계값 low=0.3/high=0.7을 기본값으로**: 실측 데이터가
  없는 상태라 CP1의 "파티션 수 32" 결정과 같은 원칙(넉넉하게/합리적으로 시작 → 실측 후 조정)을
  적용했다. 전부 `application.yml`에서 코드 수정 없이 조정 가능.
- **화이트리스트/블랙리스트를 DB가 아니라 콤마 구분 설정값으로**: 이 프로젝트에 계좌 마스터 데이터
  저장소가 없다 — 운영 정책 저장을 위한 별도 스토리지를 새로 만드는 건 CP5 범위를 벗어난다.
  `RuleEngine` 생성자에서 CSV를 파싱해 `Set<String>`으로 캐시(`ModelClientConfig`의 생성자-`@Value`
  패턴과 동일한 스타일).
- **CP5 관측 확장(Prometheus 액션 분포 카운터, end-to-end latency, Grafana 대시보드)은 이번
  범위에서 제외**: CP1~CP4 전부 "기능 구현 세션"과 "-observability 세션"을 분리해온 패턴을 그대로
  따랐다. docs/PERFORMANCE_MEASUREMENT.md CP5 표의 항목들은 다음 `backend/decision-ensemble-
  observability` 세션 대상으로 남겨둠.

## 막혔던 문제와 해결 방법

- **실 브로커 e2e 검증을 이번엔 의도적으로 생략함**: `docker ps`로 확인해보니 공유
  docker-compose 스택(Kafka/Redis/Prometheus/Grafana)이 이미 다른 세션에 의해 5분 전부터 떠
  있었고, `netstat`으로 이미 점유된 포트(18080 등)와 여러 개의 java.exe 프로세스를 확인했다 —
  `backend/model-client-concurrency-fix` 브랜치가 지금 이 인프라를 쓰고 있을 가능성이 높다. 그
  브랜치의 작업이 정확히 "동시 부하 상황에서 Circuit Breaker 복구"라는 타이밍에 민감한 검증이라,
  내가 같은 `application-id`(`fds-v2-streams-app`)로 새 앱 인스턴스를 띄우면 Kafka Streams
  리밸런싱을 유발해 그 세션의 측정을 오염시킬 위험이 있었다. 별도 application-id/consumer-group으로
  내 검증만 격리하는 방법도 검토했지만, 이번 세션에서는 위험을 아예 안 지는 쪽을 택하고
  `./gradlew test`(단위 테스트 22건, 전부 목 기반이라 공유 인프라와 무관)로 검증을 한정했다. 실제
  Kafka/Redis/TorchServe 대상 e2e는 공유 인프라가 비는 다음 세션으로 미룸 — 정직하게 기록.

## 완료 후 확인 방법 (실제로 수행함)

1. `./gradlew test` — 신규 16건 + 기존 수정 1건 포함 전체 통과 확인
   (`RuleEngineTest` 7, `EnsembleFraudDecisionServiceTest` 5, `FraudDecisionEventListenerTest` 2,
   `FraudDecisionControllerTest` 2, `AccountFeatureStoreSinkListenerTest` 6 — 전부
   tests/skipped/failures/errors XML로 직접 확인).
2. 코드 리딩으로 재확인: `AccountFeatureStoreSinkListener.onFeatureUpdate`에서 이벤트 발행이
   SET/RPUSH/TRIM/EXPIRE **이후**에 위치하는지(레이스 컨디션 방지의 핵심 전제) 직접 확인.

## 다음에 이어서 할 일

- **실제 Kafka/Redis(+가능하면 TorchServe)로 e2e 검증**: 공유 인프라가 비는 시점에 계좌 하나로
  거래를 연속 발행 → 로그에 `CP5 판정 결과` 라인이 자동으로 찍히는지, `GET
  /api/fraud-decision/{accountId}`(측정용 플래그 켜고) 결과와 로그가 일치하는지 확인.
- **CP5 관측 확장**(`backend/decision-ensemble-observability`): 액션별 분포 Prometheus Counter,
  end-to-end latency(수집→판정), 앙상블 결합 연산 latency, Grafana 패널, CP5 k6 시나리오(정상
  90% + 이상 패턴 10% 혼합).
- **docs/BACKEND.md에 CP4(model-client) 구현 스펙 소급 기록** — 이번 세션 조사 중 발견한 문서
  누락. CP5 스펙도 이번 세션 내용으로 함께 추가하면 좋음.
- **화이트리스트/블랙리스트를 설정 파일이 아닌 저장소 기반으로 전환할지 검토**: 지금은 재배포 없이
  못 바꾼다 — 운영 정책이 자주 바뀐다면 Redis Set이나 별도 관리 API가 필요.
- **`backend/model-client-concurrency-fix` 머지 후, `RuleBasedFallbackScorer` 흡수 여부 재검토**:
  그 브랜치가 정리되면 CP4/CP5 규칙 스코어러를 하나로 합칠지(코드 중복 감소) 다시 판단.
