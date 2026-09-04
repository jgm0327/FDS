# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, backend/model-client-observability, session-01

## 이번 세션에서 다룬 질문/요청

- (backend/model-client PR #9 완료 후 이어서) "다음 작업 진행해줘" → CP1~CP3가 각자
  "observability 확장" 브랜치를 따로 뒀던 패턴을 그대로 이어서, CP4 model-client의
  Resilience4j 지표를 Prometheus/Grafana에 연결 + k6 장애 주입 테스트 작성

## 변경/결정된 내용

- **`ModelClientConfig`**: `CircuitBreaker` 단독 빈 대신 `CircuitBreakerRegistry`를 거치도록
  리팩터링하고, `TaggedCircuitBreakerMetrics`로 Micrometer에 바인딩 (resilience4j-micrometer
  코어 모듈만 추가, 여전히 Spring Boot auto-configuration 스타터는 안 씀).
- **`TorchServeModelInferenceClient`**: `fds.fraud.score.count`(태그 source=MODEL/FALLBACK)
  카운터와 `fds.fallback.scorer.latency` 타이머를 직접 계측 — 둘 다
  docs/PERFORMANCE_MEASUREMENT.md CP4 표에서 "자체 계측"이라고 명시된 항목에 대응.
- **`monitoring/grafana/provisioning/dashboards/json/cp4-model-client.json`**: 6개 패널
  (서킷브레이커 상태, 실패율, 호출 결과별 발생률, MODEL/FALLBACK 비율, 폴백 응답 latency,
  TorchServe 성공 호출 latency).
- **`k6/cp4-model-client-fault-injection-test.js`**: 계좌 풀을 시간 간격을 벌린 정상 거래로
  워밍업한 뒤 `/api/fraud-score/{accountId}`를 반복 조회, 실행 중 운영자가 수동으로
  `torchserve --stop`을 실행해 장애를 주입하는 시나리오 (CP3 TTL 오버라이드 검증과 같은
  "k6는 부하만, 장애 주입은 수동" 패턴).
- **`ModelClientConfig`의 `permittedNumberOfCallsInHalfOpenState`를 3으로 명시 설정**
  (resilience4j 기본값 10 대신) — 아래 "막혔던 문제" 참고, 실측으로 발견한 문제에 대한
  대응.
- 단위 테스트 갱신: `TorchServeModelInferenceClientTest`가 새 카운터/타이머를 검증하도록
  `SimpleMeterRegistry`를 실제로 사용.

## 설계 의도 및 트레이드오프

- **CircuitBreakerRegistry 경유**: `TaggedCircuitBreakerMetrics`가 레지스트리 단위로 모든
  CircuitBreaker를 자동 탐지해서 지표를 바인딩하므로, 나중에 CircuitBreaker가 늘어나도
  배선을 그대로 재사용할 수 있다.
- **CircuitBreaker 지표만으로는 부족해서 커스텀 카운터/타이머를 추가함**: Resilience4j
  지표(`resilience4j_circuitbreaker_calls_seconds` 등)는 "호출이 성공/실패했는지"는 보여주지만
  "호출부가 실제로 어떤 스코어를 냈는지"(MODEL vs FALLBACK)까지는 직접 보여주지 않는다.
  CP4 성능 측정표의 "폴백이 얼마나 자주 발생하는지"를 정확히 답하려면 이 둘을 분리해서
  계측해야 한다고 판단.
- **k6 장애 주입을 스크립트 안에서 자동화하지 않음**: TorchServe 프로세스 종료는 k6가 제어할
  수 있는 대상이 아니므로, CP3 세션에서 세운 "k6는 부하만, 장애 주입은 운영자가 수동으로"
  원칙을 그대로 재사용했다.

## 막혔던 문제와 해결 방법 (이번 세션의 핵심)

### 1. TorchServe/torchserve --stop pid 파일 문제 (반복)

CP4 세션에서 이미 겪었던 문제가 이번에도 반복됐다 — 빠르게 재시작을 반복하다 보니
`.model_server.pid`가 다른 프로세스에 잠겨 `PermissionError`가 여러 번 났다. 매번 프로세스를
`taskkill`하고 pid 파일을 직접 지우는 식으로 대응했지만, 근본적으로 이 환경(Windows,
빈번한 프로세스 재시작)에서는 신뢰할 수 없는 방식이라는 걸 다시 확인했다 — 다음에는
`--stop`의 성공 여부를 `curl /ping`으로 항상 재확인하는 습관이 필요하다.

### 2. (핵심 발견, 미해결) 동시 부하 자체가 Circuit Breaker를 열어버리는 현상

k6로 10 VU 동시 부하를 걸자, **TorchServe가 실제로는 멀쩡히 살아있는 상태에서도** 부하
시작 후 불과 2~3초 만에 Circuit Breaker가 OPEN으로 전환되는 걸 반복 관측했다 (아래 실측
타임라인 참고). 원래 의도한 "TorchServe를 죽여야 OPEN된다"는 시나리오와 다르게, **동시성
자체가 이미 장애처럼 보이는** 상황이었다.

**실측 타임라인** (앱을 갓 재시작하고 순차 호출 5회로 CLOSED 상태 확인한 직후):
```
16:24:23~16:24:32  state=CLOSED, failure_rate=N/A (호출 수 부족)   ← k6 시작 전
16:24:32           k6 10 VU 부하 시작
16:24:36           state=OPEN, failure_rate=50.0                  ← 불과 4초 만에 OPEN
16:24:39 ~ 16:24:58 (TorchServe는 이 구간 내내 실제로 살아있었음)  state=OPEN 계속 유지
16:24:58           (예정대로) TorchServe 강제 종료
```
TorchServe를 진짜로 내리기도 전에 이미 OPEN이었고, 살아있는 22초 동안 단 한 번도 CLOSED로
복구되지 않았다.

**검증**: 같은 상황에서 k6(동시 10건)를 멈추고 **순차 호출**(한 번에 1건씩)로 바꾸자
정상적으로 HALF_OPEN → CLOSED로 복구되는 걸 확인했다. 즉 TorchServe 자체 문제가 아니라
"동시에 많은 요청이 몰릴 때"만 재현되는 문제다.

**세운 가설과 대응**: TorchServe가 모델 워커 1개(`Default workers per model: 1`, ai/README.md
TorchServe 배포 절 참고)로만 뜨기 때문에, HALF_OPEN으로 전환되는 순간에도 k6의 10개 VU가
동시에 "시험 호출"을 쏟아부어 같은 병목(단일 워커 큐잉 → timeout-ms 300ms 초과)이 반복
재현되고, 이게 회복 시도 자체를 계속 실패시켜 OPEN에서 못 벗어나는 악순환이 아닐까
추정했다. `permittedNumberOfCallsInHalfOpenState`를 기본값 10에서 3으로 낮춰서 회복 시도의
동시 부하 자체를 줄이는 완화책을 적용했다.

**미해결로 남긴 부분 (정직하게 기록)**: 이 완화책을 실제 k6 동시 부하 상황에서 재현
검증하려 했으나, 그 과정에서 (a) torchserve pid 파일 문제로 장애 주입 자체가 실패하거나
(b) 여러 차례의 재시작으로 누적된 orphan 프로세스/포트 상태 때문에 환경이 지저분해져서,
"수정 후 동시 부하에서도 깨끗하게 복구된다"는 걸 명확히 재현하는 데는 실패했다. 심지어
TorchServe를 전혀 내리지 않은 상태(정상 기동 확인 + 순차 워밍업 10회 후)에서도 k6 10 VU를
걸자 다시 100% FALLBACK이 나온 경우도 있었다 — 이는 아래 두 가설 중 하나(혹은 둘 다)로
추정되지만 이번 세션에서 확정하지 못했다:
  1. TorchServe 단일 워커가 진짜로 10 동시 요청을 못 버틴다 (그렇다면 워커 수를 늘려야 함).
  2. 이 로컬 환경 자체의 문제 — 세션 내내 앱/TorchServe를 십수 번 재시작하면서 쌓인
     TIME_WAIT 소켓, orphan 프로세스, JVM의 HttpURLConnection 커넥션 풀 상태 등이 측정을
     오염시켰을 가능성 (실제로 이 세션 후반부에 java.exe 프로세스 4개가 동시에 남아있는 걸
     발견해서 전부 강제 종료함).

## 다음에 이어서 할 일

- **동시 부하 상황에서의 Circuit Breaker 복구를 "깨끗한" 환경에서 재검증**: 세션/앱/
  TorchServe를 전부 처음부터 새로 띄운 상태에서 k6 부하 테스트를 딱 한 번만 실행해서
  (재시작 반복 없이) 이번에 세운 두 가설 중 무엇이 맞는지 확인.
- **TorchServe 워커 수 증설 검토**: 위 가설 1번이 맞다면, `ai/pytorch-sequence-model`
  쪽에서 `torch-model-archiver`/`torchserve` 기동 시 `--workers`를 1보다 크게 설정하는
  방안을 CP4 성능 측정(추론 latency, 배치 처리량)과 함께 재검토.
- **torchserve --stop의 pid 파일 신뢰성 문제**: 매 세션 반복되는 문제라, 다음엔
  `torchserve --stop` 대신 프로세스를 직접 `taskkill`하고 `/ping`으로 확인하는 방식을
  기본 절차로 문서화(`ai/README.md` 갱신 후보).
- **Grafana 대시보드 실측 스크린샷**: 이번엔 위 문제로 "정상 동작" 스크린샷을 못 남겼다 —
  재검증 후 `docs/performance-results/`에 추가.
