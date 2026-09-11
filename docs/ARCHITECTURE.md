# FDS v2 아키텍처 설계

## 문제 재정의

기존 버전: 거래 1건 → 즉시 판단 (단건 판단의 한계)
새 버전: 거래 1건 → 그 계좌의 최근 거래 흐름 전체를 반영해서 판단 (시퀀스 기반)

## 전체 파이프라인 (5단계)

```
거래 이벤트 수집 (Kafka, 계좌ID 파티셔닝)
        ↓
실시간 시퀀스 집계 (Kafka Streams, 슬라이딩 윈도우)
        ↓
온라인 피처 스토어 (Redis, 계좌별 피처 벡터)
        ↓
PyTorch 시퀀스 모델 서빙 (LSTM/Transformer, TorchServe)
        ↓
판정 및 대응 (규칙 엔진 + 모델 앙상블)
```

## 1. 거래 이벤트 수집

- 프로듀서: `ProducerRecord<>("transaction-events", accountId, event)`
- 파티션 키 = accountId. 같은 계좌의 이벤트는 항상 같은 파티션 → 같은 Kafka Streams 태스크 → 순서 보장.
- **핫 파티션 문제**: 초고빈도 계좌(PG사 등)가 특정 파티션에 트래픽을 몰아줄 수 있음.
  - ~~대응 1: Salting~~ → **구현 완료** (`backend/kafka-salting`, `AccountShardKey`/
    `ShardedAccountActivityProcessor`/`AccountActivityMergeProcessor` 참고). 고빈도로 지정된
    계좌만 "accountId#shardIndex" 형식으로 여러 파티션에 분산 발행 → CP2에서 샤드별 병렬 집계
    (Stage 1) → 계좌 단위 재파티션 + 재집계(Stage 2)로 원래 계약(`AccountFeatureVector`)을 그대로
    복원. 일반 계좌(미지정)는 샤드 1개로 고정되어 기존 동작과 100% 동일함을 회귀 테스트로 확인.
  - 대응 2: 파티션 수를 여유 있게 잡고(예: 32~64개) 컨슈머 스케일아웃 — **효과 없음을 실측으로
    확인**: Kafka는 파티션 하나를 컨슈머 그룹 내 스레드 하나만 처리할 수 있어서, 컨슈머 인스턴스를
    늘려도 단일 핫 파티션의 처리량 한계는 그대로다. 진짜 해법은 salting뿐.
  - 원칙: 파티션 수는 운영 중 늘리면 키-파티션 매핑이 깨지므로 최초 설계 시 여유 있게 잡을 것.
  - **실측 결과**(2026-09-10, `docs/sessions/..._backend-kafka-salting_...` 참고): 동일 조건
    (~307 req/s, 50초, 계좌 1개)에서 salting 적용 전 컨슈머 랙이 파티션 1개에 최대 10,348건까지
    쌓였고, 적용 후에는 8개 서브샤드가 6개 파티션으로 흩어져 파티션당 최대 랙이 857건으로
    줄었다(최악 케이스 기준 약 12배 개선). 병합 결과의 `recentWindowCount`도 실제 발행 건수와
    정확히 일치해서 재집계 정합성을 확인했다.
  - **트레이드오프**: `lastTxGapSec`/`countryChanged`는 여러 샤드의 도착 타이밍에 따른 근사값이다
    (실측 중 gap이 -1로 나온 사례 1건 관측 — 사소한 역전, 이미 CP4 model-client 쪽에 있는 음수
    gapSec 클램프로 방어됨). `recentWindowCount`/`amountRatio`는 합/평균이라 순서 무관하게
    정확하다. 이 근사는 salting 대상으로 명시적으로 지정한 소수 고빈도 계좌에만 적용되고, 일반
    계좌는 전혀 영향받지 않는다.
  - **의도적으로 미룸 — 워터마크 기반 재정렬**: 위 근사를 없애려면 "이 계좌의 모든 샤드로부터
    시각 T 이전 이벤트는 다 도착했다"를 보장하는 워터마크(그리고 유예시간 동안의 버퍼링)가
    필요하다. Kafka Streams는 이걸 `TimeWindows`/`SessionWindows`용 grace period로만 제공해서
    (우리 구조는 구간형 윈도우가 아니라 계좌당 계속 갱신되는 단일 누적 상태라 그대로 못 씀)
    직접 구현해야 하고, 샤드 하나가 조용해지면 그 샤드의 워터마크가 안 올라가 전체가 막히는
    문제(Flink의 idle source 처리와 동일한 부류)까지 별도로 풀어야 한다. 무엇보다 워터마크
    방식은 본질적으로 "확실해질 때까지 기다렸다 확정"하는 방식이라, 정확도를 얻으려면 하필
    실시간 판단이 제일 급한 salting 대상 계좌(트래픽 폭주 = 카드 도용 검증 공격의 전형적
    패턴)의 출력을 지연시켜야 한다 — 이 프로젝트의 핵심 가치(실시간성)와 충돌하는 트레이드오프라
    데이터 없이 미리 설계하지 않기로 함. 실제 운영에서 gap/countryChanged 오차 빈도를 관측한
    뒤, 필요하면 별도 브랜치로 재검토.

## 2. 실시간 시퀀스 집계

- Kafka Streams가 계좌별 담당 태스크를 두고, 각 태스크가 로컬 State Store(RocksDB, 임베디드)에 최근 거래 기록을 유지.
- 슬라이딩 윈도우 기능으로 "최근 5분/30분/1시간" 단위 통계 자동 계산:
  - 최근 N분간 거래 횟수
  - 평소 대비 이번 거래 금액 배율 (z-score 또는 단순 배율)
  - 마지막 거래 이후 경과 시간
  - 최근 이용 국가/디바이스 변화 여부
- **장애 대비**: State Store의 모든 변경은 changelog topic에도 기록. 태스크가 죽으면 다른 인스턴스가 changelog를 replay해서 State Store를 복원.
- **train-serving skew 방지**: 이 집계 로직을 학습용 배치 파이프라인과 서빙용 스트리밍 파이프라인이 동일한 코드로 공유해야 함.

## 3. 온라인 피처 스토어 (Redis)

- 키: `feature:account:{accountId}`
- 값: 계산이 끝난 최종 피처 벡터 (JSON 또는 Hash)
  ```json
  {"recent_5min_count": 7, "amount_ratio": 15.2, "last_tx_gap_sec": 12, "country_changed": true}
  ```
- 저장 방식: 개별 거래 기록이 아니라 **집계 결과만** 저장. 새 거래마다 같은 키에 덮어씀 (append 아님).
- TTL: 예) 30분. 거래가 계속 들어오면 매번 갱신되며 TTL도 리셋 → 활발한 계좌는 사실상 삭제 안 됨. 30분간 거래가 없으면 자동 삭제 (의미 없는 데이터 정리).

## 4. PyTorch 시퀀스 모델 서빙

- 입력: Redis에서 조회한 피처 벡터 → 정규화/패딩 전처리
- 모델: LSTM 또는 소규모 Transformer 인코더, TorchServe로 서빙
- 출력: 이상거래 확률 (0~1)
- **장애 대응**: Resilience4j Circuit Breaker + 타임아웃. 모델 서버 응답 지연/실패 시 규칙 기반 기본 스코어로 폴백하여 전체 판정 흐름이 막히지 않도록 함.
- **REST vs gRPC 실측 비교** (`backend/model-client-grpc-benchmark`) — "REST 먼저, gRPC는 다음
  단계"였던 계획을 실제로 구현/실측했다. `GrpcTorchServeHttpCaller`가 기존
  `TorchServeHttpCaller` 인터페이스를 그대로 구현해서 Circuit Breaker/Bulkhead/폴백 로직은
  전혀 안 건드리고 전송 계층만 교체 가능하게 만들었고, 같은 앱에 `fds.model-serving.torchserve.protocol`
  설정 하나로 REST/gRPC를 전환하며 k6로 동일 조건(4 VU, TorchServe 워커 4개) 부하를 걸어
  `fds.model-serving.torchserve.caller.latency`(직렬화+네트워크 왕복만, protocol 태그로 구분)를
  비교했다.
  - **실측 결과**(2026-09-11, `docs/sessions/2026-09-11_backend-model-client-grpc-benchmark_session-01.md`
    참고): 중앙값(p50)은 gRPC가 REST보다 일관되게 낮았다(gRPC 23~26ms vs REST 31~39ms, 두 번
    반복 측정 모두 같은 방향). 반면 꼬리 지연(p95/p99)은 REST/gRPC 양쪽 다 반복 측정 사이의
    편차가 너무 커서(REST만도 두 번 실행에서 p95가 75ms→149ms로 거의 2배 차이) 어느 쪽이
    낫다고 결론 내릴 수 없었다 — 공유 개발 머신(WSL2+Docker+다른 프로세스들)의 시스템 노이즈가
    신호보다 크다고 판단.
  - **결론**: "gRPC가 무조건 더 빠르다"가 아니라 "중앙값은 gRPC가 유리하다는 신호가 있지만,
    꼬리 지연 비교는 이 측정 환경에서는 확정할 수 없다"는 게 정직한 결론이다. 실제 운영 환경
    (전용 리소스, 반복 측정 다수 회)에서 재검증이 필요하다는 걸 명시적으로 남긴다.
  - **REST를 그대로 기본값으로 유지**: 위 결론이 gRPC 전환을 확신 있게 정당화하지 못하고,
    REST가 이미 기존 API 게이트웨이/디버깅 툴체인과 자연스럽게 맞물리는 이점(디버깅 시
    curl 하나로 확인 가능, 스키마 버저닝 부담 없음)이 있어 기본값은 바꾸지 않았다 —
    `GrpcTorchServeHttpCaller`는 `fds.model-serving.torchserve.protocol=grpc`로 언제든 켤 수
    있는 옵션으로 남겨둔다.

## 5. 판정 및 대응

- 모델 확률 스코어 + 규칙 엔진(화이트리스트, 하드룰) 평가를 앙상블로 결합
  - 하드룰: OR 조건으로 즉시 반영 (예: 블랙리스트 계좌 → 모델 확률과 무관하게 즉시 차단)
  - 애매한 구간: 가중합으로 세분화된 스코어 산출
- 임계값 비교로 고/중/저 위험 구간 분류
- 최종 액션: 허용 / 추가인증(OTP 등) / 차단 — 이진 판정이 아닌 3단계로 오탐 완충
- 판정 결과 + 추후 확정 라벨을 로깅 → 재학습 피드백 루프의 입력 데이터로 사용 (concept drift 고려)
- **애매한 구간 가중치/임계값 산정** — 원래 "모델이 시퀀스 전체를 보니 규칙보다 믿을 만하다"는
  정성적 직관으로 0.7:0.3(모델:규칙), 0.3/0.7(low/high)을 하드코딩했으나, `ai/ensemble-weight-tuning`
  브랜치(`ai/pytorch_sequence_model/tune_ensemble.py`)에서 CP4 합성 라벨 데이터로 그리드서치를
  돌려 실측으로 검증했다. 3단계 액션(허용/추가인증/차단) x 실제 라벨의 조합별 "비용"을
  가정해 기대 비용을 최소화하는 조합을 찾는 방식 — 단순 F1으로는 "사기를 추가인증으로 거른 것"과
  "완전히 놓친 것"을 구분할 수 없어서다.
  - **실측 결과**(2026-09-11, `docs/sessions/2026-09-11_ai-ensemble-weight-tuning_session-01.md`
    참고): test set(3,000건, 사기 481건) 기준, 기존 하드코딩값(0.7/0.3, 0.3/0.7)의 기대 비용
    0.1240 대비, 그리드서치로 찾은 값(모델weight=0.95/규칙weight=0.05, low=0.25/high=0.45)의
    기대 비용은 0.0817 — 약 34% 낮음. 사기 완전차단율도 93.35% → 98.34%로 개선, 정상거래
    마찰율(추가인증+차단)은 1.23% → 0.95%로 오히려 낮아짐(사기 완전누출율은 1.04%→1.25%로
    소폭 상승 — "사기를 추가인증으로만 거르는 것"보다 "완전 차단 또는 완전 허용"으로 더 확실히
    가르는 쪽이 이 비용 가정 하에서 총비용이 낮다고 판단한 결과, 아래 "코드 리뷰로 잡힌 비용
    가정 버그" 참고).
  - **규칙weight가 0이 아니라 0.05인 이유**: `ModelInferenceClient`가 서킷브레이커 오픈/타임아웃
    시 반환하는 폴백 확률 자체가 `RuleBasedFallbackScorer`의 출력이다(`FraudScore.SOURCE_FALLBACK`)
    — 즉 규칙 신호는 정상 상황에서도 "가중합의 작은 한 항"으로, 모델 장애 시에는 "대체 입력"으로
    시스템에 이중으로 반영된다.
  - **코드 리뷰로 잡힌 비용 가정 버그**: 처음 이 섹션에 적었던 값은 `CostWeights`의
    `fraud_step_up`(4.0)이 `normal_block`(6.0)보다 작게 설정돼 있었다 — 문서에 적어둔 의도
    ("사기를 추가인증으로만 거르는 게 정상거래를 차단하는 것보다 나쁘다")와 실제 코드의 숫자가
    반대였던 것. `/code-review`가 지적해서 `fraud_step_up=8.0`으로 바로잡고 그리드서치를 다시
    돌렸다 — 위 실측 결과는 수정된 값 기준이다. 처음 리포트했던 modelWeight=1.0/ruleWeight=0.0
    조합은 이 버그가 있던 비용함수로 뽑힌 값이라 폐기했다.
  - **트레이드오프**: 이 결과는 합성 데이터의 분포와 스크립트가 가정한 비용표(사기 완전누출이
    가장 나쁘다는 등 상대적 크기 가정)를 전제로 한다 — 실제 라벨 데이터가 쌓이면 비용표와
    가중치 모두 재검증이 필요하다.

## 6. 피드백 루프 및 재학습 (`backend/feedback-loop`)

핵심 설계 원칙 6번("피드백 루프")의 구현. 이 프로젝트는 실제 은행 라벨(사기 확정/정상 확인)
데이터가 없으므로, "판정 로그 + 지연 도착하는 라벨을 조인해서 재학습용 데이터셋을 만드는" 파이프라인
자체를 시뮬레이션으로 구현한다. 위 1~5번이 실시간 판정 경로라면, 이건 그 옆에 붙는 오프라인/비동기
경로다 — 판정 자체를 막지 않는다.

- **새 Kafka 토픽을 만들지 않는다**: CP1~CP4가 "토픽이 계약"이라는 패턴을 쓴 이유는 서로 다른
  컨슈머 그룹이 독립적으로 구독해야 했기 때문이다(`docs/WORKTREE_SETUP.md`). 지금 판정 로그를
  구독할 외부 컨슈머가 없는 상태에서 토픽부터 만드는 건 YAGNI다 — CP5가 "레이스 회피" 때문에
  Kafka 컨슈머 그룹 대신 애플리케이션 이벤트를 택했던 것과 결이 같다. `FraudDecisionEventListener`가
  판정 직후, 같은 프로세스 안에서 `FeedbackDecisionRecorder`를 호출해 Redis에 기록한다(나중에
  외부 감사/분석 컨슈머가 필요해지면 이 지점에 토픽 발행을 추가하면 된다).
- **라벨은 "지금" 계산하고 "나중에" 공개한다**: 실제 은행 업무도 사기 여부 자체는 거래가 일어난
  순간 이미 정해져 있고, 그걸 확인(신고/조사/이의제기 처리)하는 데 시간이 걸릴 뿐이다. 그래서
  `SimulatedLabelHeuristic`이 판정 시점에 라벨을 즉시 계산해 Redis에 저장해두고, 지연 시간이 지나면
  `FeedbackLabelScheduler`가 이미 계산된 라벨을 데이터셋으로 "공개"만 한다(재채점하지 않음).
  - **휴리스틱은 판정 액션/모델 확률을 참조하지 않는다** — 참조하면 "모델이 예측한 걸 그대로
    라벨로 되먹임"하는 순환 오류가 되어 재학습이 무의미해진다. 대신
    `ai/pytorch_sequence_model/data/synthetic.py`가 정의한 3가지 이상 패턴(금액 급증 ≥3배, 짧은
    간격의 연속 거래 ≤30초, 국가변경+금액증가 ≥1.5배)을 시퀀스 자체에서 재현하고, 대칭적 flip
    노이즈(기본 5%, "조사관도 완벽하지 않다"는 가정)를 더한다. **이건 명백한 가정이다** —
    `ai/pytorch_sequence_model/tune_ensemble.py`의 `CostWeights`가 이미 "실제 비용 데이터가 없어
    가정임을 명시"해온 것과 같은 태도다.
- **Redis pending 저장 + 정렬집합 인덱스**: `feedback:pending:{decisionId}`(JSON, TTL) +
  `feedback:pending:index`(ZSET, score=라벨 공개 예정 시각 epoch초). 스케줄러가
  `ZRANGEBYSCORE(-inf, now)`로 만기된 항목만 조회한다 — `KEYS` 전체 스캔을 피하는 표준 패턴.
- **출력은 JSONL 파일**(`data/feedback/labeled-dataset.jsonl`, `ai/artifacts`처럼 커밋하지 않는
  로컬 산출물), Kafka 토픽이 아니다 — AI 쪽이 Python Kafka 클라이언트 의존성을 새로 추가하지 않아도
  된다. **알려진 한계**: 단일 인스턴스 전제라 파일 append에 프로세스 간 락이 없다 — 운영 규모라면
  공유 스토리지나 Kafka 토픽으로 바꿔야 한다.
- **레코드 스키마는 TorchServe wire 스키마와 맞춘다** — `transactions:
  [{amountRatio, gapSec, countryChanged, merchantCategory}]`를 그대로 써서, AI 쪽이 이 JSONL을
  기존 `TransactionStep`/`AccountSequence` 스키마로 필드 변환 없이 파싱할 수 있게 했다.
- **손상된 레코드는 재시도하지 않고 버린다**: pending 레코드 파싱/데이터셋 파일 쓰기가 실패해도
  즉시 Redis에서 제거한다 — 완전한 DLQ보다 "실패하면 버리고 계속 진행"이 이번 토이 프로젝트
  범위에서는 더 합리적인 트레이드오프라고 판단했다.
- **실측 확인**(2026-09-11, `docs/sessions/2026-09-11_backend-feedback-loop_session-01.md` 참고):
  로컬 docker-compose(Kafka/Redis)에 실제로 거래 이벤트를 발행해 CP1→CP2→CP3→CP5→CP6 전체
  파이프라인이 동작하는 걸 확인했다. 정상 시퀀스는 label=0, 금액 8배 급증+국가변경 시퀀스는
  label=1로 정확히 시뮬레이션됐고, 지연(3~5초로 임시 단축) 후 `labeled-dataset.jsonl`에 조인된
  레코드가 정확히 나타났다.
- **다음 세션 범위(`ai/retraining-pipeline`)**: 이 JSONL을 로드해 합성 데이터와 블렌딩,
  기존 `best_model.pt` 대비 비용 함수(`tune_ensemble.py`와 동일 방식) 기준 개선이 없으면 승격하지
  않는 concept-drift 체크. 아직 구현되지 않았다.

## 아직 논의/구현 필요 사항 (TODO)

- [ ] 파티션 수/컨슈머 인스턴스 수 구체적 산정 기준
- [x] ~~Salting 적용 시 재집계 로직 상세 설계~~ → `backend/kafka-salting`에서 구현 완료 (위 1번
      "핫 파티션 문제" 참고)
- [x] ~~TorchServe 배포 및 gRPC/REST 인터페이스 결정~~ → `backend/model-client-grpc-benchmark`에서
      둘 다 구현하고 실측 비교 완료 (위 4번 "PyTorch 시퀀스 모델 서빙" 참고). 결론: REST를
      기본값으로 유지, gRPC는 옵션으로 남김 — 꼬리 지연 비교는 측정 환경 노이즈 때문에
      결론을 못 내려서 운영 환경 재검증이 남아있음.
- [x] ~~앙상블 가중치/임계값 초기값 산정 방법~~ → `ai/ensemble-weight-tuning`에서 그리드서치로
      구현 완료 (위 5번 "판정 및 대응" 참고). 실제 라벨 데이터 확보 시 재검증 필요는 남아있음.
- [x] ~~재학습 파이프라인(라벨 지연, concept drift 대응) 구체 설계~~ → 백엔드 절반(판정 기록 →
      라벨 시뮬레이션 → 조인 → 데이터셋 파일 출력)은 `backend/feedback-loop`에서 구현 완료(위
      6번 참고). AI 쪽(데이터셋으로 실제 재학습 + concept-drift 승격 체크)은 `ai/retraining-pipeline`
      브랜치로 남아있음.
- [ ] CP1~CP7 체크포인트를 이 5단계 경계에 재배치
