# CP4 — PyTorch 시퀀스 모델

`docs/ARCHITECTURE.md` 4단계("PyTorch 시퀀스 모델 서빙")에 대응. 모델 구조/학습 스크립트만
다루는 `ai/pytorch-sequence-model` 브랜치 범위이며(`docs/WORKTREE_SETUP.md` 분리표 참고),
Spring Boot → TorchServe 호출 클라이언트는 별도 브랜치(`backend/model-client`)에서 다룬다.

## 실행 방법

```bash
cd ai
pip install -r requirements.txt

python -m unittest discover -s tests         # 단위 테스트 (패딩/모델 shape/합성 데이터 검증)
python -m pytorch_sequence_model.train      # 합성 데이터로 학습, ai/artifacts/best_model.pt 생성
python -m pytorch_sequence_model.evaluate    # 시퀀스 모델 vs 단건 베이스라인 비교 출력
python -m pytorch_sequence_model.serving.export  # TorchScript export, ai/artifacts/traced_model.pt 생성
```

`ai/artifacts/`는 `.gitignore`에 포함되어 커밋되지 않는다 — 아래 "왜 학습 산출물을 커밋하지
않는지" 참고.

## 입력/출력 계약 (backend/model-client가 참고할 인터페이스)

모델은 계좌 하나의 최근 거래 시퀀스(오래된 것 -> 최신 순, 최대 30건, `config.MAX_SEQ_LEN`)를
받아 사기 확률(0~1) 스칼라 하나를 낸다. 거래 1건의 필드:

| 필드 | 의미 | CP2 `AccountFeatureVector`와의 관계 |
|---|---|---|
| `amountRatio` | 이 거래 금액 / 그 시점까지의 평소 금액 | 동일 정의 |
| `gapSec` | 직전 거래 이후 경과 시간(초) | `lastTxGapSec`과 동일 정의 (첫 거래는 0) |
| `countryChanged` | 직전 거래와 국가가 다르면 true | 동일 정의 |
| `merchantCategory` | 가맹점 카테고리 문자열 | CP2에는 없는 필드 — 이 모델이 추가로 요구 |

TorchServe REST 요청/응답 형식은 `pytorch_sequence_model/serving/handler.py` 상단 docstring
참고.

## 핵심 설계 결정

1. **모델 = LSTM (Transformer 아님)**: `pytorch_sequence_model/model.py` 상단 docstring에 근거를
   자세히 남겼다 — 요약하면 시퀀스가 짧아(≤30) Transformer의 병렬화 이점이 작고, CP4 성능
   측정 목표(추론 latency)와 TorchScript trace 단순성 면에서 LSTM이 유리하다고 판단.
2. **pack_padded_sequence 대신 gather로 마지막 유효 타임스텝 추출**: 정확도상 미세한 손해
   (패딩 스텝도 LSTM에 통과시킴) 대신 TorchScript trace를 단순하게 유지 — `model.py` 참고.
3. **정규화(log1p)를 모델 forward 안에 포함**: 전처리 로직이 학습과 서빙 양쪽에서 어긋날
   위험(train-serving skew, ARCHITECTURE.md 2번 항목이 강조하는 문제와 같은 종류)을 원천
   차단하기 위해, 핸들러는 원시값(raw amountRatio/gapSec)만 넘기고 스케일링은 항상
   TorchScript에 포함된 `forward()`가 담당하게 했다.
4. **합성 데이터의 이상 패턴을 정상 분포와 일부 겹치게 설계**: `data/synthetic.py` 참고.
   완벽히 분리되는 가짜 데이터로 학습하면 "단건 규칙 하나로도 100% 잡히는" 자명한 문제가
   되어, 시퀀스 모델의 존재 이유(여러 약한 신호의 맥락적 결합)를 증명할 수 없다.
5. **학습 산출물(체크포인트/TorchScript)을 커밋하지 않음**: 합성 데이터 생성부터 학습까지
   전부 결정적(seed 고정)이라 스크립트만 있으면 언제든 동일한 결과를 재현할 수 있다.
   바이너리 모델 파일을 git에 넣는 대신, 실제 학습/평가 결과 수치는 세션 로그
   (`docs/sessions/`)에 실측값으로 남긴다 — CP1~CP3가 스크린샷/실측 로그를 남긴 것과 같은 원칙.

## 평가 방법론 — 왜 "단건 베이스라인"과 비교하는가

`docs/PERFORMANCE_MEASUREMENT.md`의 "개선 전/후 비교 방법"은 정확도/탐지력 관점과 성능 비용
관점을 함께 보라고 명시한다. `evaluate.py`는 정확도/탐지력 관점을 담당 — 같은 테스트셋에서
① 시퀀스 전체(LSTM)를 보는 모델과 ② 마지막 거래 1건만 보는 로지스틱 회귀(기존 FDS의 단건
판단을 근사)의 AUC/Precision/Recall을 나란히 출력한다. 성능 비용 관점(추론 latency, 서킷
브레이커 폴백 전환 시간)은 TorchServe 배포 후 k6 + Prometheus로 별도 측정한다(아래 TODO).

### 실측 결과 (합성 데이터 기준, 2026-09-04)

테스트셋 3,000건(사기 481건, 16.0%)에서 `python -m pytorch_sequence_model.evaluate` 실행 결과:

| | AUC | Precision | Recall | F1 |
|---|---|---|---|---|
| 시퀀스 모델 (LSTM, 최근 최대 30건) | 0.9996 | 0.9711 | 0.9792 | 0.9752 |
| 베이스라인 (마지막 거래 1건, 로지스틱 회귀) | 0.9235 | 0.6027 | 0.7443 | 0.6660 |

AUC 차이 +0.0761, F1 차이 +0.31 — 같은 신호를 시퀀스 맥락과 함께 보는 것만으로 탐지력이
크게 개선됨을 합성 데이터 기준으로 확인. 전체 confusion matrix 등 상세 수치는
`docs/sessions/2026-09-04_ai-pytorch-sequence-model_session-01.md` 참고.

## TorchServe 배포 — 실제로 기동해서 확인함

Python 3.14 + torchserve 0.12.0 조합이 공식 지원 매트릭스에 없어 설치가 안 될 것으로
예상했지만, 실제로 설치/기동/REST 호출까지 전부 됐다 (pyyaml 누락 이슈 하나만 있었음 —
아래 "막혔던 문제" 참고). 로컬에서 재현하는 방법:

```bash
cd ai
python -m pytorch_sequence_model.serving.export   # ai/artifacts/traced_model.pt 생성

torch-model-archiver \
  --model-name fds-sequence-model \
  --version 1.0 \
  --serialized-file artifacts/traced_model.pt \
  --handler pytorch_sequence_model/serving/handler.py \
  --export-path model_store \
  --force

# handler.py가 `from pytorch_sequence_model.config import ...`로 절대 임포트하므로,
# .mar 안에는 handler.py 파일 자체만 들어가고 패키지 구조는 안 들어간다 — 로컬 실행 시
# ai/ 디렉터리를 PYTHONPATH에 넣어줘야 handler가 config.py를 찾는다.
# (운영 배포 시에는 handler.py를 자기 완결적으로 만들거나 --extra-files로 config.py를
# 같이 넣는 방식으로 바꿔야 한다 — 지금은 로컬 검증 목적의 임시 해법)
PYTHONPATH=$(pwd) torchserve --start \
  --model-store model_store \
  --models fds-sequence-model=fds-sequence-model.mar \
  --disable-token-auth --ncs

curl -X POST http://127.0.0.1:8080/predictions/fds-sequence-model \
  -H "Content-Type: application/json" \
  -d '{"accountId":"acc-1","transactions":[{"amountRatio":1.02,"gapSec":1800,"countryChanged":false,"merchantCategory":"GROCERY"}]}'

torchserve --stop
```

실측 결과(세션 로그에도 기록): 정상 패턴 시퀀스 -> `fraudProbability: 0.0003`,
금액 급증+연속 거래+국가변경이 겹친 시퀀스 -> `fraudProbability: 1.0`.

CP4 성능 측정 표(추론 latency p50/p95/p99, 서킷브레이커 폴백 전환)는 k6 부하 + Prometheus
연동이 필요해 이번 세션 범위 밖 — 다음 세션 TODO.

## 백엔드 연동 시 남는 숙제 — CP2/CP3가 스텝별 원본 이력을 안 남긴다

이 모델은 계좌의 "최근 거래 각각"을 스텝으로 받는데, 현재 CP2(`AccountFeatureVector`)/CP3
(Redis)는 **집계 결과 1개(최신 스냅샷)만** 저장하도록 설계되어 있다(`docs/ARCHITECTURE.md`
3번 "저장 방식: 개별 거래 기록이 아니라 집계 결과만 저장"). 즉 지금 파이프라인 그대로는
`backend/model-client`가 이 모델에 넘길 "최근 N건 시퀀스"를 만들 방법이 없다.

이번 세션은 `ai/pytorch-sequence-model` 브랜치 범위(합성 데이터로 병렬 진행 가능, `docs/
WORKTREE_SETUP.md` 분리표)라 이 간극을 여기서 메우지 않았다 — 대신 옵션만 남겨둔다:
- CP2 State Store에 이미 있는 "최근 윈도우 타임스탬프 목록" 등을 스텝 단위로 함께 발행하도록
  `account-feature-updates` 계약을 확장 (`backend/kafka-streams-topology` 쪽 작업)
- 또는 Redis에 계좌별 최근 N건을 별도 리스트(`feature:account:{id}:recent`)로 함께 유지
  (`backend/redis-feature-store` 쪽 작업, TTL/메모리 트레이드오프 재검토 필요)

어느 쪽이든 CP2/CP3의 기존 설계 결정("집계 결과만 저장")과 정면으로 부딪히므로, 다음
백엔드 세션에서 트레이드오프를 다시 논의해야 한다.
