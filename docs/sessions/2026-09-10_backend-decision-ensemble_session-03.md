# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-10, backend/decision-ensemble (main에 병합된 상태에서 검증), session-03

## 이번 세션에서 다룬 질문/요청

- "그럼 순서대로 진행해줘" — 남은 작업 목록 중 1순위였던 **CP5 실제 Kafka/Redis e2e 검증**
  (session-01/02가 의도적으로 미뤄둔 항목)을 진행.
- 이 세션은 코드 변경 없이 순수 검증만 했다 — 그래서 별도 worktree/브랜치를 새로 만들지 않고
  main에서 직접 검증하고, 결과만 이 로그로 남긴다.

## 변경/결정된 내용

로컬 docker-compose 스택(Kafka/Redis/Prometheus/Grafana) + `bootRun`으로 main HEAD를 띄우고,
`POST /api/transactions`로 실제 시나리오를 재현해서 CP5 전체 경로를 처음으로 실측 확인했다.

**확인된 것 (전부 실제로 동작함)**:

1. **자동 트리거**: 거래 발행 → CP2(집계) → CP3(Redis 저장 + 이벤트 발행) → CP5가 사람이 아무것도
   안 해도 자동으로 판정 실행. `GET /api/fraud-decision/{accountId}`를 수동 호출할 필요 없음.
2. **비동기 실행 확인**: 판정이 `fraud-decision-N` 스레드(session-02에서 추가한 전용 풀)에서
   실행되고, Kafka 컨슈머 스레드(`ntainer#0-N-C-1`)와 분리되어 있는 것을 스레드 이름으로 직접 확인.
3. **TorchServe 미기동 시 우아한 폴백**: TorchServe를 아예 안 띄운 상태(이번 세션 범위 밖 —
   자동 트리거 배선 자체가 목적이라 모델 서버까지는 안 띄움)로 뒀더니, `ModelInferenceClient`가
   실패를 감지해 규칙 기반 폴백으로 정상 전환. `predict()`가 예외를 던지지 않는다는 계약이
   실제 인프라에서도 유지되는 것 확인.
4. **3단계 액션 전부 실측**:
   - ALLOW: 정상 거래, combinedScore 낮음
   - STEP_UP_AUTH: 애매한 구간(금액배율은 높지만 하드룰 임계치 미만)
   - BLOCK: 하드룰 즉시 반영 — `triggeredRules=["extreme-amount-ratio-with-country-change"]`,
     **modelProbability/modelSource가 null** — 설계대로 하드룰이 걸리면 모델을 아예 호출하지
     않는 것까지 확인(TorchServe 호출 로그 자체가 안 찍힘).
5. **`GET /api/fraud-decision/{accountId}`가 자동 트리거 로그와 일치**: 같은 계좌를 다시 조회하니
   같은 액션/스코어/triggeredRules를 반환(재계산이지만 규칙 엔진이 결정적이라 값이 같음).

## 설계 의도 및 트레이드오프 — 이번 검증에서 새로 발견한 것

- **누적 평균 기준(amountRatio)이 "한 번의 초대형 이상 거래" 이후 급격히 둔감해짐**: 하드룰
  임계치(기본 20배)를 실제로 트리거해보는 과정에서 우연히 발견함. 계좌에 이미 거래 이력이 있는
  상태에서 거래를 연속으로 키워가며(5백만 → 5천만 → 9천만 → 20억) 하드룰을 노려봤는데, 매번
  실패했다. 원인: CP2의 "평소 금액"이 전체 기간 누적 평균이라, 한 번 초대형 거래(20억)가 들어가는
  순간 평균 자체가 그 거래를 포함해 확 뛰어버려서, **바로 다음 거래는 아무리 커도 새 평균 대비
  비율이 20배를 넘기 어려워진다.** 반대로 이력이 거의 없는 신선한 계좌(정상 거래 1건 → 그다음
  50배 스파이크+국가변경)에서는 하드룰이 즉시 정확히 걸렸다.
  - 이건 CP2 세션(`2026-09-03_backend-kafka-streams-topology_session-01.md`)이 이미 설계 시점에
    "평소 금액 = 전체 기간 누적 평균... EWMA나 최근 N건 평균이 더 정교하지만 MVP는 단순 평균으로
    시작, 다음 개선 후보로 남김"이라고 명시했던 바로 그 한계인데, 이번에 실측으로 **구체적인 공격
    패턴**(첫 거래는 무조건 못 걸리고, 한 번 큰 거래가 성공하면 그다음부터는 오히려 하드룰을
    피하기 쉬워짐 — "이미 커진 계좌"가 "정상 계좌"보다 하드룰에 덜 걸리는 역설)으로 확인됨.
  - 이번 세션 범위(검증)에서 고치지는 않았다 — CP2의 집계 로직 자체를 바꿔야 하는 더 큰 작업이라,
    "다음에 이어서 할 일"로 구체적 실측 근거와 함께 남긴다.

## 막혔던 문제와 해결 방법

- **Docker Desktop이 꺼져 있었음**: `Start-Process`가 기본 경로(`C:\Program Files\Docker\...`)를
  못 찾음 — 실제 설치 경로는 사용자 로컬(`%LOCALAPPDATA%\Programs\DockerDesktop\Docker Desktop.exe`)
  이었음. 그 경로로 직접 실행해서 해결.
- **TorchServe 호출이 403 Forbidden으로 실패**: 원인은 버그가 아니라 포트 충돌 — 기본
  `TORCHSERVE_BASE_URL`이 `localhost:8080`인데, 이 포트를 로컬의 무관한 다른 프로젝트
  (`pingbell-app`)가 이미 점유하고 있어서 그 앱의 Whitelabel 에러 페이지가 대신 응답한 것.
  TorchServe를 이번 세션에서 띄우지 않기로 한 결정과 맞물려, 오히려 "모델 서버가 전혀 없어도
  폴백이 정상 동작하는가"를 검증하는 유효한 시나리오가 되어 그대로 진행함.
- **하드룰이 계속 안 걸림**: 처음엔 amountRatio 임계치(20배) 계산을 잘못 예상해서 여러 번
  실패 → 위 "새로 발견한 것" 항목 참고. 신선한 계좌로 바꿔서 재현에 성공.

## 확인 방법 (실제로 수행함)

```
docker compose up -d
SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 \
  FDS_REDIS_PORT=16379 FDS_DECISION_QUERY_ENDPOINT_ENABLED=true \
  FDS_FEATURE_STORE_QUERY_ENDPOINT_ENABLED=true ./gradlew bootRun
```

- 계좌 `acc-cp5-e2e`: 정상 거래 3건 → 전부 ALLOW(폴백), 자동 트리거 로그 확인.
- 계좌 `acc-cp5-block-test`: 기존 이력이 있는 상태에서 거래를 반복적으로 키워도 하드룰 회피됨
  (누적 평균 둔감화 현상 재현).
- 계좌 `acc-cp5-block-clean`: 정상 거래 1건 → 50배 스파이크+국가변경 거래 1건 → **BLOCK**,
  `triggeredRules=["extreme-amount-ratio-with-country-change"]`, 모델 호출 자체가 스킵됨을
  로그로 확인.
- `GET /api/fraud-decision/acc-cp5-block-clean`이 자동 트리거 결과와 액션/스코어 일치.
- 검증 후 앱 종료, `docker compose down`으로 인프라 정리.

## 다음에 이어서 할 일

- **"평소 금액" 계산을 단순 누적 평균에서 EWMA/최근 N건 평균으로 개선** (CP2 세션이 이미 남긴
  후보, 이번에 구체적 실측 근거 추가됨) — 지금 방식은 "한 번 성공한 큰 거래가 다음 하드룰 탐지를
  더 어렵게 만드는" 역설적 취약점이 있음.
- 남은 작업 목록의 나머지: CP5 관측 확장, docs/BACKEND.md CP4/CP5 스펙 소급 기록, 이중 Redis
  읽기 레이스, TorchServe 워커/Bulkhead 동기화, 음수 gapSec 근본 원인, salting, 재학습 피드백
  루프 등 (이전 세션 로그들 참고).
