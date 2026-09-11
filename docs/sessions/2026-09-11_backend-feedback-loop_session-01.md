# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-11, backend/feedback-loop, session-01

## 이번 세션에서 다룬 질문/요청

- "다음 작업 후보" 3개(재학습 피드백 루프 / 워크트리 housekeeping / 문서 정리) 중 "재학습
  피드백 루프"를 선택. 요구사항: CP5 판정 로그 + 지연 도착하는 라벨을 조인해서 재학습에 쓰는
  파이프라인. 실제 은행 라벨 데이터가 없으므로 라벨 지연 도착 자체를 시뮬레이션해야 함(규모가
  크다고 미리 메모돼 있었음).
- 시작 전 plan 모드로 기존 구조(CP5 판정 트리거 방식, Redis/Kafka 사용 패턴, AI 쪽 학습 데이터
  스키마, 문서 컨벤션)를 먼저 조사하고 설계를 확정한 뒤 구현.
- 스코프를 백엔드 절반(판정 기록 → 라벨 시뮬레이션 → 조인 → 데이터셋 파일 출력)으로 한정하고,
  AI 쪽(실제 재학습 + concept-drift 승격 체크)은 `ai/retraining-pipeline`으로 다음 세션에 넘기기로
  계획 단계에서 결정.

## 변경/결정된 내용

- **`com.fdsv2.feedback` 패키지 신설**: `FeedbackTransactionStep`, `PendingDecisionRecord`,
  `FeedbackDatasetRecord`, `FeedbackKeyBuilder`, `SimulatedLabelHeuristic`,
  `FeedbackDecisionRecorder`, `FeedbackLabelScheduler`, `FeedbackDatasetWriter`, `FeedbackConfig`.
- 기존 `FraudDecisionEventListener`에 CP6 연동 추가 — `decide()` 성공 직후
  `AccountRecentSequenceReader.readRecentSteps(accountId)`로 판정에 실제 쓰인 시퀀스를 다시 읽어
  `FeedbackDecisionRecorder.record(...)` 호출. 실패는 별도 try/catch로 격리.
- `application.yml`에 `fds.feedback.*` 설정 추가(pending 키/TTL, 라벨 시뮬레이터 지연·flip
  확률·폴링 주기, 데이터셋 출력 경로).
- 테스트: `SimulatedLabelHeuristicTest`(7), `FeedbackDecisionRecorderTest`(4),
  `FeedbackLabelSchedulerTest`(5), `FeedbackDatasetWriterTest`(2) 신규 + 기존
  `FraudDecisionEventListenerTest`에 2건 추가. 전체 `./gradlew test` 93건 통과.
- 문서: `docs/ARCHITECTURE.md`에 "6. 피드백 루프 및 재학습" 섹션 신설 + TODO 체크,
  `docs/BACKEND.md`에 "6차 구현 범위 (CP6)" 섹션 추가, `docs/PERFORMANCE_MEASUREMENT.md`에 CP6
  지표 섹션 추가.
- 세션 시작 시점에 이전 세션이 남긴 것으로 보이는 `pr11.diff.txt`(이미 병합된 PR의 diff, untracked)
  를 발견해 삭제 — 이번 작업과 무관한 잔여 파일로 판단.

## 설계 의도 및 트레이드오프

- **새 Kafka 토픽을 만들지 않고 Redis + 같은 프로세스 내 호출로 구현**: CP1~CP4가 "토픽이 계약"
  패턴을 쓴 이유는 서로 다른 컨슈머 그룹이 독립적으로 구독해야 했기 때문인데, 지금 판정 로그를
  구독할 외부 컨슈머가 없다. 토픽부터 만드는 건 YAGNI라고 판단했고, CP5가 레이스 회피를 위해
  Kafka 컨슈머 그룹 대신 애플리케이션 이벤트를 택했던 선례와 같은 결의 판단이다. 대신 CP6이
  구독할 대상이 생기면(외부 감사/분석 시스템 등) 이 지점에 토픽 발행을 추가하면 되도록 설계
  경계를 남겨뒀다.
- **라벨은 판정 직후 계산, 공개만 지연**: 처음에는 "스케줄러가 만기 시점에 재채점"하는 설계를
  생각했으나, 실제 은행 업무를 다시 생각해보면 "사기 여부는 거래가 일어난 순간 이미 정해져 있고,
  그걸 확인하는 데 시간이 걸릴 뿐"이다 — 라벨 "계산"과 "공개"를 분리하는 게 더 현실을 정확히
  반영하고, 구현도 더 단순해진다(스케줄러가 원본 시퀀스를 다시 들고 있을 필요가 없음). plan 단계
  설계를 구현 중에 이렇게 다듬었다.
- **라벨 휴리스틱이 판정 결과(액션/모델 확률/규칙 점수)를 절대 참조하지 않음**: 참조하면 "모델이
  예측한 걸 그대로 라벨로 되먹임"하는 순환 오류가 되어 재학습이 무의미해진다. 대신
  `ai/pytorch_sequence_model/data/synthetic.py`가 이미 정의해둔 3가지 이상 패턴(금액 급증,
  짧은 간격의 연속 거래, 국가변경+금액증가)을 그대로 시퀀스에서 재현했다 — 나중에 이 시뮬레이션
  라벨로 재학습한 모델을 기존 `evaluate.py`/`tune_ensemble.py`의 합성 데이터 결과와 나란히
  비교하려면 "이상 패턴의 정의"부터 같아야 하기 때문이다.
- **flip 노이즈(기본 5%)로 "조사관도 완벽하지 않다"를 반영**: `tune_ensemble.py`의 `CostWeights`가
  이미 "실제 비용 데이터가 없어 가정임을 명시"해온 것과 같은 태도로, 이 휴리스틱도 명백한 가정임을
  코드/문서 양쪽에 반복해서 남겼다.
- **출력을 Kafka 토픽이 아니라 JSONL 파일로**: AI 쪽이 Python Kafka 클라이언트를 새로 추가하지
  않아도 되고, `ai/artifacts`처럼 "재생성 가능한 로컬 산출물"로 자연스럽게 취급된다. 대신 단일
  인스턴스 전제(파일 append에 프로세스 간 락 없음)라는 한계를 문서에 명시했다.
- **레코드 스키마 필드명을 TorchServe wire 스키마(`handler.py`)에 맞춤**: `transactions:
  [{amountRatio, gapSec, countryChanged, merchantCategory}]` 그대로 써서, 다음 세션(AI 쪽)이
  필드 변환 없이 기존 `TransactionStep`/`AccountSequence`로 파싱할 수 있게 계약을 미리 맞췄다.
- **손상된 pending 레코드는 재시도 없이 버림**: 완전한 DLQ(dead-letter queue)를 만들 수도 있었지만,
  이 토이 프로젝트 범위에서는 "실패하면 버리고 계속 진행"이 poison-pill로 영원히 재시도 로그만
  쌓는 것보다 합리적인 트레이드오프라고 판단했다 — `FeedbackLabelScheduler` 클래스 javadoc에
  명시.

## 막혔던 문제와 해결 방법

- **`gapSec` null 클램프가 라벨 휴리스틱을 오염시킬 뻔함**: 처음에는 `SimulatedLabelHeuristic`이
  `FeedbackTransactionStep`(TorchServe wire 스키마, `gapSec`이 null 불허라 첫 거래는 0.0으로
  클램프됨)을 입력으로 받는 설계였다. 그런데 "간격 30초 이하면 burst" 조건과 "첫 거래는
  gapSec=0.0"이 겹쳐서, **모든 시퀀스의 첫 거래가 무조건 burst로 오판되는 버그**가 될 뻔했다 —
  코드를 쓰기 전에 이 상호작용을 미리 알아채고, 휴리스틱은 클램프 이전의 원본 `RawFeatureStep`
  리스트(`lastTxGapSec`이 null이면 "첫 거래"라는 의미가 그대로 보존됨)를 받도록 설계를 바꿨다.
  `SimulatedLabelHeuristicTest`에 이 케이스를 회귀 테스트로 명시적으로 남겼다
  (`첫_거래의_lastTxGapSec_null은_burst로_오판하지_않는다`).
- **TorchServe가 안 떠 있는 상태에서 e2e 검증**: 이번 세션은 WSL TorchServe를 새로 기동하지
  않았다 — CP5는 이미 서킷브레이커 타임아웃 시 규칙 기반 폴백으로 정상 동작하도록 설계돼 있어서
  (`modelSource=FALLBACK`), CP6 검증에는 영향이 없었다. 실제로 로컬 docker-compose(Kafka/Redis)만
  띄운 상태로 전체 파이프라인이 정상 동작하는 걸 확인했다.

## 확인 방법 (실제로 수행함)

1. `./gradlew test` — 신규 4개 테스트 클래스(18건) + 기존 `FraudDecisionEventListenerTest` 추가
   2건 포함, 전체 93건 전부 통과.
2. **로컬 docker-compose(Kafka `localhost:19092`, Redis `localhost:16379`)로 실제 e2e 검증** —
   라벨 지연을 3~5초로 임시 단축(`FDS_FEEDBACK_LABEL_MIN_DELAY_SECONDS=3`,
   `FDS_FEEDBACK_LABEL_MAX_DELAY_SECONDS=5`, `FDS_FEEDBACK_LABEL_POLL_INTERVAL_MS=2000`)해서
   `POST /api/transactions`로 두 시나리오 재현:
   - 정상 거래 1건(금액비 1.0, 국가 불변) → CP5 `action=ALLOW` → CP6 `label=0`으로 정확히
     시뮬레이션되어 지연 후 `data/feedback/labeled-dataset.jsonl`에 기록됨.
   - 정상 거래 1건 + 금액 8배 급증·국가변경 거래 1건 → CP5가 규칙 폴백으로 `action=BLOCK`
     (TorchServe 미기동, `modelSource=FALLBACK`)을 내렸고, CP6 라벨은 이와 무관하게 계산되어
     `label=1`로 정확히 기록됨 — 판정 액션과 시뮬레이션 라벨이 서로 다른 계산 경로임을 실측으로
     확인(순환 오류가 없다는 설계 의도의 실제 검증).
   - 두 경우 모두 지연 시간이 지나기 전에는 `feedback:pending:*` Redis 키가 존재하다가, 지연 후
     사라지고 JSONL에 나타나는 것을 확인(라벨 "지연 공개" 동작 검증).
3. 검증 후 로컬 앱 프로세스 종료, `data/`(gitignore 대상) 정리.

## 다음에 이어서 할 일

- `ai/retraining-pipeline` 브랜치: `data/feedback/labeled-dataset.jsonl` 로더(→
  `AccountSequence`/`TransactionStep`), 합성 데이터와 블렌딩해 재학습, 기존 `best_model.pt` 대비
  비용 함수(`tune_ensemble.py`와 동일 방식) 기준 개선이 없으면 승격하지 않는 concept-drift 체크.
- `docs/ARCHITECTURE.md` TODO의 "CP1~CP7 체크포인트 재배치"는 이번 범위 밖(별도 문서 작업
  후보로 계속 남아 있음).
- 워크트리 housekeeping(머지 완료된 브랜치용 워크트리 4개 정리)도 아직 미착수.
