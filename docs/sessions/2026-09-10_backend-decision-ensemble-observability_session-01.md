# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-10, backend/decision-ensemble-observability, session-01

## 이번 세션에서 다룬 질문/요청

- "진행해줘" — 남은 작업 목록에서 CP5 e2e 검증(session-03) 다음 순서였던 **CP5 관측 확장**
  (`docs/PERFORMANCE_MEASUREMENT.md` CP5 표: End-to-end latency, 액션별 분포, 앙상블 결합 연산
  latency + Grafana 패널 + k6 혼합 시나리오)을 진행.

## 변경/결정된 내용

CP1~CP4와 동일한 패턴("기능 PR"과 "-observability PR" 분리)을 그대로 따라, 새 worktree/브랜치로
진행.

- **`fds.decision.action.count`** (Counter, 태그 `action`): `EnsembleFraudDecisionService.decide()`
  에서 즉시결정/앙상블결정 두 경로 공통으로 기록 — 액션별 분포.
- **`fds.decision.ensemble.combine.latency`** (Timer): `ensembleDecision()`에서
  `modelInferenceClient.predict()`(HTTP 왕복, CP4 대시보드에서 이미 측정 중) **호출은 제외**하고,
  가중합 계산 + `toAction()` 임계값 비교만 감싼다.
- **`fds.decision.e2e.latency`** (Timer): `FraudDecisionEventListener`에서
  `FeatureStoreUpdatedEvent.occurredAt()`(CP3 Redis 쓰기 완료 시점)부터 판정 완료까지 측정 —
  전용 스레드풀 큐잉 시간까지 포함.
- **`monitoring/grafana/provisioning/dashboards/json/cp5-decision-ensemble.json`** 신규 —
  4개 패널(액션별 분포, e2e latency p50/p95/p99, 앙상블 결합 연산 latency p50/p99, 판정 처리량).
- **`k6/cp5-decision-mixed-test.js`** 신규 — 정상 90% + 이상 패턴 10% 혼합 시나리오. 계좌마다
  고정된 "평소 국가/평소 금액대"를 부여하고, 이상 거래는 평소 대비 30~50배 금액 + 국가 변경으로
  만든다.
- 테스트: `EnsembleFraudDecisionServiceTest`에 2건 추가(액션 카운터/결합 latency 기록 확인,
  즉시결정 경로도 카운터는 기록되는지), `FraudDecisionEventListenerTest`를 e2e latency Timer
  기록까지 검증하도록 수정. 생성자 시그니처 변경(`MeterRegistry` 추가)에 따라 두 테스트 파일의
  생성 부분도 함께 수정.

## 설계 의도 및 트레이드오프

- **`combine.latency`가 `predict()` 호출을 반드시 제외해야 하는 이유**: PERFORMANCE_MEASUREMENT.md
  문구 자체가 "병목이 아닌지 확인, 보통 매우 짧아야 정상"이다 — 여기에 HTTP 왕복까지 포함시키면
  이 지표가 CP4의 추론 latency와 사실상 같아져서 "결합 연산 자체는 병목이 아니다"라는 걸 증명할
  수 없게 된다. `Timer.record(Supplier)`로 감싸는 범위를 `predict()` 호출 **다음**부터로 명확히
  한정했다.
  - **트레이드오프**: `predict()`가 `ensembleDecision()` 안에서 Timer 밖에 남아있어서, 메서드
    하나를 눈으로만 보면 "왜 이 호출은 재지 않지?"가 바로 안 보일 수 있다 — 클래스 javadoc에
    이유를 명시해서 보완.
- **e2e latency를 CP1~CP3까지 포함해서 재지 않은 이유**: 각 구간은 이미 자기 대시보드
  (CP1/CP2)가 있다. 여기서 또 재면 같은 구간을 두 번 측정하는 중복이 되고, 정작 "CP5 자체가
  느려지면 그게 보이는" 좁고 정확한 신호를 얻기 어려워진다. 대신 CP3가 Redis 쓰기 직후 찍는
  `FeatureStoreUpdatedEvent.occurredAt()`을 기준점으로 삼아서, "CP3 이후 ~ CP5 완료"라는 CP5의
  실제 책임 구간만 정확히 잰다.
- **k6 시나리오가 이상 거래 강도(30~50배)를 CP1의 결정 방식(계좌마다 고정 평소 금액대)과
  맞춘 이유**: session-03(e2e 검증)에서 "누적 평균이 한 번의 큰 거래 이후 급격히 둔감해진다"는
  걸 발견했다 — 이 스크립트는 그 문제를 고치는 게 아니라 있는 그대로 관측하는 게 목적이라,
  일부러 아주 정교하게 회피 시나리오를 만들지 않고 "정상/이상"이 뚜렷이 구분되는 단순한 패턴으로
  실측 자체에 집중했다.

## 막혔던 문제와 해결 방법

- 없음 — CP1~CP4가 이미 만들어둔 Prometheus/Grafana 프로비저닝 구조(대시보드 JSON 파일 추가만
  하면 자동 로드)를 그대로 재사용했고, Micrometer Timer/Counter 패턴도 기존 코드(CP4의
  `fds.fallback.scorer.latency` 등)와 동일하게 맞춰서 특별히 헤맨 지점은 없었다.

## 확인 방법 (실제로 수행함)

```
./gradlew test                        # 신규/수정 테스트 포함 전체 통과
docker compose up -d
SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 \
  FDS_REDIS_PORT=16379 ./gradlew bootRun
k6 run k6/cp5-decision-mixed-test.js  # 20 VU, 1분, 11,635건
```

- Prometheus 직접 쿼리로 3개 신규 지표 전부 실측 확인:
  - `sum by (action) (fds_decision_action_count_total)` → ALLOW 9496 / STEP_UP_AUTH 1202 /
    BLOCK 937 (총 11,635건 중) — 정상 90%/이상 10% 설계와 대체로 일치(이상 거래 일부가 반복
    누적되며 STEP_UP_AUTH/BLOCK 비율이 10%보다 다소 높게 나옴).
  - e2e latency p95 ≈ **4.19ms** (SLA 500ms 대비 충분히 여유).
  - 앙상블 결합 연산 latency p99 ≈ **0.99ms** — "병목이 아니다"라는 문서 기대치와 일치.
- Grafana `FDS v2 - CP5 Decision Ensemble`(uid `fds-v2-cp5`) 대시보드가 자동 프로비저닝되어
  API로 조회 확인(`/api/dashboards/uid/fds-v2-cp5`).
- 검증 후 앱 종료, `docker compose down`으로 인프라 정리.

## 다음에 이어서 할 일

- 남은 작업 목록의 나머지: `docs/BACKEND.md` CP4/CP5 스펙 소급 기록, 이중 Redis 읽기 레이스,
  TorchServe 워커/Bulkhead 동기화, 음수 gapSec 근본 원인, "평소 금액" 계산 개선(EWMA/최근 윈도우
  평균), salting, 재학습 피드백 루프.
- PR 리뷰(`/code-review`) 후 머지.
