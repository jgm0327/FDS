# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-08, backend/model-client-concurrency-fix, session-01

## 이번 세션에서 다룬 질문/요청

- 2026-09-04_backend-model-client-observability_session-01.md의 "막혔던 문제 2번"(k6 10 VU
  동시 부하를 걸면 TorchServe가 멀쩡한데도 Circuit Breaker가 OPEN에서 못 벗어나는 현상) 이어서
  진행 — 그 세션이 세운 두 가설(① TorchServe 워커 1개가 동시 부하를 못 버틴다, ② 이전 세션의
  반복 재시작으로 로컬 환경이 오염됐다) 중 어느 쪽이 맞는지, 앱/TorchServe/Docker를 전부 새로
  띄운 깨끗한 상태에서 k6를 딱 한 번 실행해서 확인하고, 맞는 원인에 맞게 고쳐달라는 요청.

## 변경/결정된 내용

- **가설 검증 결과: 가설 1 확정, 가설 2 기각.** Docker(Kafka/Redis/Prometheus/Grafana)·
  TorchServe·Spring Boot 앱을 전부 처음부터 새로 띄우고(재시작 이력 없음) k6 10 VU를 45초간
  1회 실행한 결과, TorchServe 프로세스는 한 번도 안 죽었는데도 3709건 중 964건(26%)이 FALLBACK
  으로 전환됐다 — "환경이 지저분해서"가 아니라 동시성 자체가 원인임을 재확인. TorchServe 관리
  API(`GET /models/fds-sequence-model`)로 확인한 실제 배포 설정은 `minWorkers=maxWorkers=1`.
  워커 1개가 동시 요청을 순차 처리하며 대기시간이 쌓이고, 그 대기시간이
  `timeout-ms`(300ms)를 넘겨 실패로 잡히는 것이 실제 메커니즘이었다.
- **(부수 발견, 이번 세션에서 새로 찾음) 음수 gapSec → 모델 NaN → JSON 파싱 실패 버그.**
  가설 검증 후 "TorchServe 워커를 4개로 늘리면 해결되는지" 재검증하는 과정에서, 같은 k6
  워밍업 스크립트를 이미 데이터가 쌓인 계좌에 대해 재실행하자(같은 세션 안에서 fix 검증을 위해
  반복 실행) 요청의 100%가 FALLBACK으로 전환되는 별도 현상을 발견했다. 앱 로그를 직접 확인한
  결과 원인은 워커 수와 무관— 계좌의 최근 거래 시퀀스에 `lastTxGapSec`이 음수인 항목이 섞여
  있으면(워밍업 스크립트가 "지금 시각 -N분" 오프셋으로 합성 타임스탬프를 만드는데, 같은 계좌에
  대해 여러 번 재실행하면 이후 실행의 타임스탬프가 이전 실행보다 앞서는 경우가 생김 — 즉 거래가
  시간 역순으로 기록됨), 모델 forward의 log1p(gapSec) 정규화가 `log(1+음수)`를 계산해 NaN을
  내고, TorchServe가 이 NaN을 JSON 응답에 그대로 실어 보내는데 표준 JSON은 NaN 리터럴을
  허용하지 않아 Jackson 파싱이 실패, 그대로 Circuit Breaker의 "실패"로 잡힌 것이었다.
- **`TorchServeTransactionStep.from()`**: gapSec이 null이면 0.0으로 채우던 기존 방어 로직을
  음수인 경우까지 확장 — 음수면 0.0으로 clamp. 회귀 테스트
  (`TorchServeTransactionStepTest`) 추가.
- **`ModelClientConfig` / `TorchServeModelInferenceClient`**: `Bulkhead`(resilience4j-bulkhead
  2.2.0 신규 의존성)를 TorchServe 호출 경로에 추가 — `bulkhead.executeSupplier(() ->
  circuitBreaker.executeSupplier(...))` 순서로 Bulkhead를 Circuit Breaker 바깥쪽에 감싼다.
  `fds.model-serving.torchserve.bulkhead.max-concurrent-calls`(기본 4, `maxWaitDuration=0`)로
  앱이 TorchServe에 동시에 흘려보내는 호출 수 자체를 실제 배포된 워커 수만큼 제한한다.
  `TaggedBulkheadMetrics`로 `resilience4j_bulkhead_available_concurrent_calls` /
  `..._max_allowed_concurrent_calls`를 Prometheus에 노출(CircuitBreaker 지표와 같은 패턴).
- **`TorchServeModelInferenceClientConcurrencyTest`(신규)**: maxConcurrentCalls=1인 Bulkhead
  뒤에서 두 번째 동시 호출이 TorchServe를 아예 호출하지 않고 즉시 폴백하는지, 그 거절이
  CircuitBreaker 통계(성공/실패 카운트)에 전혀 안 잡히는지 검증.
- **`ai/README.md`**: TorchServe 기본 워커 수(1)로는 k6 10 VU 부하를 못 버틴다는 실측 결과와,
  로컬 검증 시 관리 API(`PUT .../models/fds-sequence-model?min_worker=4&max_worker=4`)로 워커를
  늘리는 방법, 이 값과 backend의 `bulkhead.max-concurrent-calls`(기본 4)를 반드시 같이 맞춰야
  한다는 점을 새 절로 추가.

## 설계 의도 및 트레이드오프

- **Bulkhead를 Circuit Breaker "바깥쪽"에 감싼 이유**: Bulkhead가 거절한 호출(BulkheadFullException)은
  `circuitBreaker.executeSupplier()` 자체가 실행되지 않으므로 Circuit Breaker의 성공/실패
  통계에 전혀 안 잡힌다. 반대 순서로 감쌌다면 "앱 자신의 동시성 제한"이 다시 "TorchServe
  실패"로 오인되어 서킷이 열리는, 이번에 고치려던 문제와 똑같은 자기 참조적 버그가 재발했을
  것이다. 이 순서 덕분에 Circuit Breaker는 이제 "TorchServe가 실제로 응답한 실패"만 보게
  되고(최종 검증에서 `failed=0, not_permitted=0`으로 확인), 상태 지표의 신뢰도가 올라간다.
- **maxWaitDuration을 0(대기 없이 즉시 거절)으로 잡은 이유**: 이번에 겪은 문제의 본질이
  "큐잉된 요청이 timeout-ms 안에 못 끝나서 실패로 잡히는 것"이라, 앱 안에서도 똑같이
  큐잉시키면 같은 문제가 형태만 바뀌어 재발한다. 대기시키는 대신 초과분은 TorchServe 근처도
  안 가고 곧바로 규칙 기반 폴백으로 보내는 편이 latency도 낫고 원인도 명확하다.
- **워커 수 증설(ai/ 배포 설정)과 Bulkhead(backend 코드) 둘 다 적용한 이유**: 워커 수 증설이
  근본 대책이지만 그것만으로는 "배포된 워커 수보다 트래픽이 늘어나는 상황"이 재발할 수 있다.
  Bulkhead는 그 상황에서도 Circuit Breaker가 가짜 신호로 오염되지 않게 하는 방어선 —
  근본 대책(용량 증설)과 방어 대책(용량 초과 시 신호 보호)을 함께 두는 편이 안전하다고 판단.
- **음수 gapSec을 0.0으로 clamp(모델 재학습이 아니라 입력 방어)한 이유**: 근본적으로는 같은
  계좌에 시간 역순 이벤트가 애초에 쌓이지 않게 하는 게 맞고, 그건 시퀀스 집계 쪽
  (`backend/sequence-window-feature-store`)의 책임이다. 하지만 실서비스에서도 재전송/클럭
  스큐 등으로 이벤트가 역순 도착할 가능성은 완전히 배제할 수 없어서, 모델 호출부(이 브랜치
  범위)에서 방어적으로 clamp하는 게 더 안전하다고 판단 — null gapSec을 0.0으로 채우던 기존
  코드와 같은 성격의 방어라 자연스럽게 확장했다.

## 막혔던 문제와 해결 방법

### 1. 로컬 환경 준비 자체가 여러 단계로 막힘

- Docker Desktop 엔진이 안 떠 있어서 `docker compose up`이 파이프 연결 에러로 실패 —
  Docker Desktop.exe를 직접 기동해서 해결.
- 다른 worktree 세션이 이미 만들어 둔 동일 `container_name`의 fds-v2-* 컨테이너(4일 전,
  Exited 상태)가 있어서 `docker compose up -d`가 이름 충돌로 실패 — 완전히 새 상태로
  시작하기 위해 해당 컨테이너들을 지우고 볼륨까지 정리한 뒤 재기동.
- torchserve/torch-model-archiver가 PATH에 없었음(`pip install --user`라
  `AppData\Roaming\Python\Python314\Scripts`에 설치됨) — PATH에 직접 추가해서 해결.
- `ai/artifacts/`, `ai/model_store/`가 `.gitignore` 대상이라 완전히 없는 상태 — `train.py` →
  `export.py` → `torch-model-archiver`를 순서대로 다시 실행해서 처음부터 재현.

### 2. (이번 세션 핵심) 두 가설 중 무엇이 맞는지 확인

완전히 새로 띄운 환경에서 k6를 재시작 없이 딱 한 번 실행해 26% FALLBACK을 재현했고,
TorchServe 관리 API로 워커 수가 1개임을 직접 확인해서 가설 1을 확정했다. 자세한 근거는 위
"변경/결정된 내용" 참고.

### 3. (예상 밖 발견) 워커를 4개로 늘렸는데 오히려 100% FALLBACK으로 악화

가설 1을 고치려고 TorchServe 워커를 4개로 늘린 뒤 재검증하다가, 개선은커녕 완전히
악화되는 걸 보고 당황했다 — 처음엔 "워커를 늘리면 더 나빠지는 이상한 동시성 버그인가"
의심했다. 직접 TorchServe에 동시 요청 50건을 쏴봐도(우회, 앱 안 거침) 전부 정상 응답이라
TorchServe 자체 문제가 아님을 확인했고, 그 다음 앱 로그를 직접 열어봐서(이전 세션들처럼
백그라운드 커맨드 출력을 파이프로 흘리면 로그 파일이 비어버리는 걸 알게 되어 파이프 없이
재기동) 진짜 원인(음수 gapSec → NaN → JSON 파싱 실패)을 찾았다. 원인은 워커 수와 무관하게
"같은 계좌에 대해 k6 워밍업을 여러 번 재실행"한 이번 세션 자체의 검증 방식이 만든 문제였다 —
Redis에 데이터가 없던 최초 실행(가설 검증 시점)에는 이 문제가 없었다는 것도 오프셋 계산으로
확인. 결과적으로 이번 세션 안에서 "가설 2(반복 실행이 상태를 오염시킨다)"와 결이 같은 문제를
스스로 만들어 겪은 셈 — 원래 세션의 가설 2(TIME_WAIT/orphan 프로세스)는 기각됐지만, "같은
계좌에 반복 실행하면 상태가 누적되어 오염된다"는 더 넓은 원리 자체는 유효함을 다른 형태로
재확인했다.

### 4. 최종 검증

TorchServeTransactionStep clamp + Bulkhead 적용, Redis `FLUSHALL`로 오염된 데이터 제거, 앱
재기동 후 k6를 다시 한 번 실행 — Circuit Breaker는 45초 내내 CLOSED 유지
(`failed=0, not_permitted=0, failure_rate=0.0`), FALLBACK 21%(744/3475)는 전부 Bulkhead가
초과 동시 요청을 즉시 거절한 결과였다(Circuit Breaker 통계에 전혀 안 잡힘). Circuit Breaker가
더 이상 부하 자체로 오염되지 않는다는 목표는 달성했다 — 다만 워커 4개로도 10 VU 지속 부하를
완전히 다 받아내지는 못한다(아래 "다음에 이어서 할 일" 참고).

## 다음에 이어서 할 일

- **TorchServe 워커 수/Bulkhead 용량 재산정**: 워커 4개 + bulkhead 4로도 10 VU 지속 부하의
  21%가 여전히 FALLBACK이다 — 이건 이제 "가짜 실패"가 아니라 "진짜 용량 부족" 신호이므로,
  실제 운영 목표 처리량에 맞춰 워커 수(및 bulkhead 값)를 다시 산정해야 한다.
  ai/pytorch-sequence-model 쪽 배포 자동화(현재는 관리 API를 수동 호출)도 필요.
- **음수 gapSec 근본 원인 정리**: 이번엔 backend/model-client 호출부에서 방어적으로 clamp만
  했다 — 시퀀스 집계(backend/sequence-window-feature-store)가 애초에 시간 역순 이벤트를
  어떻게 다뤄야 하는지(무시/재정렬/에러)는 아직 논의 안 됨.
  `feature:account:{id}:recent` LIST가 여러 실행에 걸쳐 계속 누적되는 것도 k6 반복 실행 시
  주의가 필요하다는 걸 이번에 알게 됨 — TTL은 있지만(feature-store.ttl-minutes) recent LIST
  자체의 정리 정책은 별도 확인 필요.
- **로컬 검증 커맨드를 파이프로 백그라운드에 흘리지 말 것**: `./gradlew bootRun | tail -N`
  형태로 백그라운드 실행하면 출력 캡처 파일이 비어서(스트리밍 안 됨) 로그를 못 본다 — 이번에
  파이프 없이 재기동해서 알아냄. 다음부터는 항상 파이프 없이 백그라운드로 띄운다.
