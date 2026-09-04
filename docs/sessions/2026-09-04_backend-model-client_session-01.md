# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, backend/model-client, session-01

## 이번 세션에서 다룬 질문/요청

- "다음 작업 착수하려면?" → CP2/CP3↔CP4 연결(PR #8) 다음 단계로 `backend/model-client` 브랜치
  착수 (Spring Boot → TorchServe 실제 호출 + Circuit Breaker)

## 변경/결정된 내용

- **`ModelInferenceClient`** 인터페이스 + **`TorchServeModelInferenceClient`** 구현체:
  계좌ID를 받아 (1) `feature:account:{id}:recent` Redis LIST를 읽고 → (2) CP4 TorchServe REST
  계약(`ai/pytorch_sequence_model/serving/handler.py`)에 맞춰 요청 JSON을 만들어 → (3) Circuit
  Breaker로 감싸 호출하고 → (4) 실패 시 규칙 기반 폴백으로 전환. 항상 `FraudScore`(스코어 +
  `source`: MODEL/FALLBACK)를 반환하며 예외를 던지지 않는다.
- **`AccountRecentSequenceReader`**: Redis LIST를 읽어 `RawFeatureStep`으로 파싱(손상된 원소는
  건너뜀). CP2 `AccountFeatureVector` 자바 타입에 의존하지 않고 이 패키지에 독립적으로 같은
  형태를 재정의 — CP3가 세운 "JSON 형태만 계약" 원칙을 여기까지 일관되게 유지.
- **`TorchServeHttpCaller`** 인터페이스 + **`RestClientTorchServeHttpCaller`** 구현체: 실제 HTTP
  호출만 떼어낸 좁은 인터페이스 — 단위 테스트에서 진짜 HTTP 서버 없이 성공/실패만 목으로 흉내낼
  수 있게 하려는 목적.
- **`RuleBasedFallbackScorer`**: 서킷브레이커 폴백 시 쓰는 임시 규칙(금액배율≥5 및/또는
  국가변경 조합 → 0.1/0.6/0.9). CP5(판정 및 대응)의 정식 규칙 엔진이 아니라 그 전까지의
  placeholder임을 명시.
- **`ModelClientConfig`**: Resilience4j `CircuitBreaker` 빈을 직접 구성(auto-configuration
  스타터 미사용, 아래 트레이드오프 참고).
- **`FraudScoreController`**: `GET /api/fraud-score/{accountId}` — CP3 `FeatureQueryController`와
  같은 패턴(기본 비활성, `fds.model-serving.query-endpoint-enabled` 플래그로 측정 시에만 활성화).
- `application.yml`: `fds.model-serving.torchserve.model-name`, `circuit-breaker.*`(실패율
  임계치/OPEN 유지시간/슬라이딩 윈도우/최소 호출수), `query-endpoint-enabled` 추가.
- 단위 테스트 9건 추가(`RuleBasedFallbackScorerTest`, `AccountRecentSequenceReaderTest`,
  `TorchServeModelInferenceClientTest` — 정상/실패/서킷오픈 시나리오 포함).

## 설계 의도 및 트레이드오프

- **worktree를 main이 아니라 `backend/sequence-window-feature-store`(PR #8)에서 분기**: 이
  브랜치가 읽는 `feature:account:{id}:recent` 자체가 PR #8에서 처음 생긴 것이라, main만으로는
  이 기능을 구현/테스트할 수 없다. PR #8이 머지되기 전까지는 **스택형 PR**(base를 main이 아니라
  `backend/sequence-window-feature-store`로 잡음)로 두고, #8이 머지된 뒤 base를 main으로
  재조정할 계획.
- **resilience4j-spring-boot3 스타터 대신 core 모듈만 사용 + 수동 빈 구성**: 이 프로젝트가 이미
  최신(4.1.1) Spring Boot를 쓰고 있어, 스타터의 auto-configuration이 그 버전과 안 맞을 위험을
  피하고 싶었다. `CircuitBreaker`를 직접 만들어 `executeSupplier`로 감싸는 정도는 코드량이
  크지 않아서, 버전 호환성 리스크보다 이 편이 낫다고 판단.
- **connect timeout과 read timeout을 하나의 설정값(timeout-ms)으로 통일**: 따로 두면 실제
  최대 대기시간이 설정값의 몇 배가 될 수 있어, "이 타임아웃 값이 곧 서킷브레이커가 열리는
  시점의 근거"라는 CP4 성능 측정표의 의도가 흐려진다.
- **TorchServeHttpCaller로 HTTP 호출부를 분리**: RestClient의 플루언트 체이닝 API를 통째로
  모킹하는 건 번거롭고 깨지기 쉽다 — 실제로 필요한 건 "성공/실패" 두 시나리오뿐이라, 인터페이스
  하나로 떼어내서 Mockito로 간단히 목킹했다.
- **RuleBasedFallbackScorer는 최소 규칙만**: CP5가 아직 없는 상태에서 폴백 경로가 완전히
  무의미한 값을 내지 않게 하는 정도로만 구현했다. 진짜 하드룰/화이트리스트/앙상블은 CP5의
  몫으로 명확히 선을 그었다.

## 막혔던 문제와 해결 방법

- **TorchServe와 Spring Boot 앱이 기본 포트(8080)를 두고 충돌**: 둘 다 기본 8080을 쓰다 보니,
  TorchServe를 먼저 띄운 상태에서 앱을 8080으로 띄우려다 실패했다(및 그 반대 순서에서도 이전
  세션에서 `TaskStop`으로 종료했다고 생각한 Spring Boot 프로세스가 실제로는 살아남아 포트를
  점유하고 있었음 — 백그라운드 셸을 죽여도 자식 JVM 프로세스가 orphan으로 남는 경우가 있다는
  걸 확인). `netstat`으로 점유 PID를 찾아 `taskkill`로 직접 종료하고, 앱은 `SERVER_PORT=8090`으로
  띄워서 해결.
- **torchserve pid 파일이 잠겨서 재기동 실패**: 이전 시도의 orphan 프로세스가 `.model_server.pid`
  파일을 잠그고 있어 `PermissionError`가 났다 — 해당 java 프로세스를 taskkill 후 pid 파일을
  직접 삭제하고 재기동해서 해결.
- **e2e 테스트에서 "정상" 계좌인데도 fraudProbability가 0.95~1.0으로 나온 문제**: 처음엔 버그를
  의심했으나, 원인은 테스트 스크립트가 여러 거래에 **같은 occurredAt 값을 재사용**한 것이었다
  — 결과적으로 `gapSec=0`이 되어, 학습 데이터의 "burst_frequency"(초 단위 간격 연속 거래)
  이상 패턴과 똑같은 신호를 만들어낸 것. 거래 간격을 30분~2시간으로 벌려서 재발행하니
  fraudProbability가 0.0389로 정상 범위로 나왔다 — **모델이 의도대로 시간 간격에 민감하게
  반응하고 있다는 걸 실측으로 확인**한 셈이기도 하다 (버그가 아니라 테스트 데이터 설계 실수).
- (부수 발견, 버그 아님) **거래 1건짜리 계좌의 예측이 불안정함**(0.9511 등): CP4 합성 데이터
  생성기(`ai/pytorch_sequence_model/data/synthetic.py`)의 정상 시퀀스 최소 길이가 3이라, 길이
  1인 시퀀스는 학습 데이터 분포 밖(out-of-distribution)이다. 실제 신규 계좌의 첫 거래는 항상
  이 상황을 맞게 되므로, ai 쪽에 최소 길이 1도 정상 분포에 포함시키거나, 이 model-client 쪽에서
  "시퀀스가 너무 짧으면 모델을 아예 안 부르고 규칙 기반으로만 판단" 하는 안전장치가 필요해
  보인다 — 다음 세션 TODO로 남김.

## 완료 후 확인 방법 (실제로 수행함)

1. `./gradlew test` — 신규 9건 포함 전체 테스트 통과.
2. 실제 TorchServe(재학습/재export/재아카이빙 후 기동, ai worktree)+Redis+Kafka로 e2e 검증:
   - 거래 3건(간격 30분~2시간, 금액 배율 1.0 근처)을 발행한 계좌 → `GET /api/fraud-score/{id}`
     결과 `{"fraudProbability":0.0389,"source":"MODEL"}` — TorchServe 실호출 경로 확인.
   - 금액 급증(700, 평소 대비 7배)+국가변경(KR→US)이 섞인 계좌 → `fraudProbability:1.0,
     source:MODEL`.
   - `torchserve --stop`으로 TorchServe를 내린 뒤 같은 두 계좌를 다시 조회 →
     `source:FALLBACK`으로 전환 확인, 정상 계좌는 `0.1`, 위험 계좌는 `0.9`로 규칙 기반 스코어가
     실제로 계좌 신호를 반영해서 나뉘는 것 확인.
3. 테스트 중 발견한 타이밍 이슈(위 "막혔던 문제" 참고)를 재현/수정하며 gapSec 민감도를 재확인.

## 다음에 이어서 할 일

- **PR #8 머지 후 이 PR의 base를 main으로 재조정** (현재는 #8 위에 쌓은 스택형 PR).
- **짧은 시퀀스(길이 1~2) 처리 정책 결정**: 모델을 아예 안 부르고 규칙 기반으로 바로 가는
  임계값을 둘지, 아니면 ai 쪽 합성 데이터에 더 짧은 시퀀스도 포함해 재학습할지 — 백엔드/AI
  양쪽 다음 세션에서 논의 필요.
- **Resilience4j Micrometer 연동**: CP1~CP3가 각자 "observability 확장" 브랜치를 따로 뒀던
  패턴 그대로, `backend/model-client-observability`에서 서킷브레이커 상태/폴백 발생률을
  Prometheus/Grafana에 연결 (docs/PERFORMANCE_MEASUREMENT.md CP4 표).
- **CP4 k6 부하테스트 + 장애 주입**: TorchServe에 인위적 지연을 주입해 서킷브레이커가 설계한
  타임아웃대로 열리는지 확인 (PERFORMANCE_MEASUREMENT.md CP4 "k6 시나리오 + Fault Injection").
- **CP5(판정 및 대응)**: `RuleBasedFallbackScorer`를 흡수/대체할 정식 규칙 엔진 + 앙상블.
