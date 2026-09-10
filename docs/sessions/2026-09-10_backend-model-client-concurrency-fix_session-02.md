# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-10, backend/model-client-concurrency-fix, session-02

## 이번 세션에서 다룬 질문/요청

- PR #12를 `/code-review` medium 강도로 리뷰(8개 관점, angle A 최종 리포트 기준 "6개가 아니라
  2개만 진짜"라고 스스로 압축) → 반영.

## 변경/결정된 내용

여러 관점에서 findings가 나왔는데, 그중 이 클래스의 핵심 계약("predict()는 항상 FraudScore를
반환하며 예외를 던지지 않는다")을 실제로 깨는 것 1개를 고쳤고, 관측 개선 1개를 덤으로 반영했다.

1. **`sequenceReader.readRecentSteps()` 호출이 try 블록 밖에 있던 문제 수정**
   (`TorchServeModelInferenceClient.predict()`). Redis 장애 시(이 PR이 막으려는 것과 같은 종류의
   동시 부하 상황에서 커넥션 풀 고갈 등으로 실제로 벌어질 수 있음) 이 예외가 Bulkhead/CircuitBreaker/
   규칙 기반 폴백을 전부 우회해서 `predict()` 밖으로 그대로 샜다. 조회를 자체 try/catch로 감싸서
   실패 시 `latestStep=null`로 곧바로 규칙 기반 폴백으로 빠지도록 수정. 회귀 테스트 1건 추가
   (`RedisConnectionFailureException` 주입 → `httpCaller`가 아예 호출 안 되고 FALLBACK 반환 확인).
2. **폴백 사유를 태그로 구분** — 기존엔 `BulkheadFullException`(용량 초과)/`CallNotPermittedException`
   (서킷 오픈)/실제 TorchServe 실패/응답 파싱 실패가 전부 같은 로그 한 줄·같은 카운터로 뭉뚱그려
   져서, 정작 이 PR이 Bulkhead를 CircuitBreaker와 분리해서 얹은 목적("용량 초과"와 "실제 실패"를
   구분)이 사람이 보는 로그/지표에서는 다시 합쳐졌다. `fallbackReason(Exception)`으로 예외 타입을
   구분해서 `fds.fraud.score.count`에 `reason` 태그(`bulkhead_full`/`circuit_open`/
   `torchserve_error`/`sequence_read_error`)를 추가하고 로그에도 포함시켰다.

## 설계 의도 및 트레이드오프

- **`fallback()` 헬퍼로 추출**: 위 두 catch 블록(Redis 조회 실패, TorchServe 호출 실패)이 공통으로
  하는 일(Timer로 폴백 스코어러 호출 + 카운터 증가 + FraudScore 조립)을 한 곳으로 모아서, "폴백
  경로는 항상 이 한 함수를 거친다"는 걸 보장했다 — 두 catch가 따로 커스터마이즈되다가 한쪽만
  카운터 태그를 빠뜨리는 식의 드리프트를 방지.
- **reason 태그를 카운터에 추가해도 기존 테스트가 안 깨지는 이유 확인**: Micrometer의
  `MeterRegistry.get(name).tag(k, v)`는 부분 일치 검색이라, 태그를 추가해도 기존
  `.tag("source", "FALLBACK")` 조회는 그대로 통과한다 — 실제로 전체 테스트를 돌려서 확인함.

## 리뷰에서 나왔지만 이번엔 반영하지 않은 항목 (이유 포함)

- **Bulkhead 기본값(4)이 TorchServe의 기본 배포(워커 1개)와 안 맞는다는 지적**: 실측 자체는 맞다 —
  session-01의 최종 검증은 TorchServe를 수동으로 4워커까지 올린 뒤에 한 것이라, 그 수동 단계를
  빠뜨린 배포에서는 여전히 원래 문제가 형태만 바꿔 재발할 수 있다. 다만 이건 이미 session-01의
  "다음에 이어서 할 일"에 "TorchServe 워커 수/Bulkhead 용량 재산정... 배포 자동화 필요"로 명시적
  으로 남겨둔 항목이라, 이번 세션에서 새로 발견한 게 아니라 이미 계획된 후속 작업이다. 제대로
  고치려면(TorchServe 관리 API를 앱 기동 시 조회해서 Bulkhead 크기를 맞추는 등) 더 큰 자동화
  작업이 필요해서 이번엔 손대지 않았다.
- **Bulkhead 지표가 CP4 Grafana 대시보드/PERFORMANCE_MEASUREMENT.md에 아직 안 실려있다는 지적**:
  CP1~CP4가 계속 "기능 PR"과 "-observability PR"을 분리해온 패턴대로, 다음 관측 확장 세션 대상.
  reason 태그 추가(위 2번)로 최소한의 구분은 이번에 확보했다.
  - **음수 gapSec을 null과 같은 sentinel(0.0)로 clamp해서 두 경우가 구분 안 된다는 지적**: 근본
  원인(시간 역순 이벤트 자체)은 `backend/sequence-window-feature-store` 책임 영역이고, 이 클래스
  경계에서 구분하려면 모델 입력 계약(`TorchServeTransactionStep`)에 새 필드를 추가해야 해서
  ai/ 쪽과 조율이 필요한 더 큰 변경이다 — session-01이 이미 "다음에 논의 필요"로 남긴 항목이라
  이번엔 반영하지 않음.
- **나머지(중첩 람다 평탄화, CircuitBreaker/Bulkhead 빈 3종 세트 반복, 테스트 픽스처 중복 등)**:
  낮은 심각도의 정리성 지적이라 보류.

## 확인 방법 (실제로 수행함)

```
./gradlew test   # 신규 회귀 테스트 1건 포함 전체 통과
```

## 다음에 이어서 할 일

- session-01의 "다음에 이어서 할 일" 그대로 유효(TorchServe 워커/Bulkhead 용량 재산정, 음수
  gapSec 근본 원인 정리 등).
- PR #11(backend/decision-ensemble)이 이 브랜치의 `TorchServeModelInferenceClient` 생성자
  시그니처(Bulkhead 인자 추가)에 의존하는 테스트를 옛날 버전으로 갖고 있다는 걸 코드 리뷰에서
  발견함 — 이 PR이 먼저 머지된 뒤 그쪽을 rebase해서 맞춰야 함(머지 순서: 이 PR 먼저).
