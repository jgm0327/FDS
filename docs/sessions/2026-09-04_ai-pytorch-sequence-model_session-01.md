# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, ai/pytorch-sequence-model, session-01

## 이번 세션에서 다룬 질문/요청

- "CP4(PyTorch 시퀀스 모델) 착수하자" — CP4 구현 시작 요청
  (원래 `backend/kafka-partitioning` worktree 세션에서 요청이 들어와, CLAUDE.md/
  WORKTREE_SETUP.md의 브랜치 분리 원칙에 따라 `ai/pytorch-sequence-model` worktree로
  세션을 전환한 뒤 진행함 — 사용자 확인 후 EnterWorktree로 전환)

## 변경/결정된 내용

- `ai/` 폴더 신설 (Python 패키지 `pytorch_sequence_model`) — CLAUDE.md 폴더 구조의
  "ai/** 브랜치 작업 결과물" 자리를 처음 채움.
- **모델 입력 계약 확정**: 계좌 하나의 최근 거래 시퀀스(오래된 것 -> 최신, 최대 30건)를
  스텝 단위로 받음. 스텝 필드 = `amountRatio`/`gapSec`/`countryChanged`(CP2
  `AccountFeatureVector`와 동일 의미론) + `merchantCategory`(CP4가 새로 요구).
  자세한 계약은 `ai/README.md`, `ai/pytorch_sequence_model/serving/handler.py` 참고.
- **모델 구조**: LSTM(hidden=64, 1층) + 가맹점 카테고리 임베딩(4차원) + 2층 MLP 헤드.
  pack_padded_sequence 대신 gather로 마지막 유효 타임스텝만 추출 (TorchScript trace
  단순화 목적). log1p 정규화를 handler가 아니라 `model.forward()` 안에 내장(train-serving
  skew 방지).
- **합성 데이터 생성기** (`data/synthetic.py`): 정상/사기 시퀀스를 각각 생성. 사기는
  정상처럼 보이는 burn-in 뒤에 마지막 1~3건에 이상 패턴(금액 급증/burst
  frequency/국가변경+소폭 금액상승) 주입. 정상 분포와 일부 겹치게 강도를 조절(완전
  분리 방지).
- **학습/평가/서빙 파이프라인 전부 구현 및 실제 실행 검증**:
  - `train.py`: 계좌 2만 건 합성, 15 epoch 학습.
  - `evaluate.py`: 시퀀스 모델 vs "마지막 거래 1건" 로지스틱 회귀 베이스라인(기존 FDS
    단건 판단 근사) 비교.
  - `serving/export.py`: TorchScript trace export.
  - `serving/handler.py`: TorchServe 커스텀 핸들러(REST 계약 문서화 포함).
- **TorchServe를 실제로 로컬에 설치/기동해서 REST 추론까지 확인함** (아래 "실제로 수행함"
  참고) — 착수 전에는 Python 3.14 비호환으로 막힐 거라 예상했으나 실제로는 됨.
- `ai/tests/` — 단위 테스트 9개 추가(패딩/길이 인코딩, 모델 출력 shape, gather가 padding
  이후 값에 영향받지 않는지, 합성 데이터 라벨 비율/재현성/시드별 차이). `python -m
  unittest discover -s tests`로 전체 통과 확인.

## 설계 의도 및 트레이드오프

- **LSTM vs Transformer**: 시퀀스가 짧고(≤30) CP4 성능 측정 목표가 추론 latency라, 병렬화
  이점이 크지 않은 Transformer 대신 더 가볍고 trace가 단순한 LSTM을 선택. 시퀀스가 훨씬
  길어지거나 장거리 의존성이 중요해지면 재검토 대상 (`model.py` docstring에 근거 기록).
- **pack_padded_sequence 미사용**: 패딩 스텝까지 LSTM에 통과시켜 약간의 연산 낭비가
  있지만(MAX_SEQ_LEN=30에서는 무시할 만함), TorchScript trace가 훨씬 단순해짐 — 서빙
  안정성을 정확도/효율 최적화보다 우선한 결정.
- **정규화를 모델 안에 내장**: CP2 세션 로그에서 강조된 train-serving skew 문제(같은
  집계 로직을 학습/서빙이 공유해야 한다는 원칙)를 이번에도 그대로 적용 — 핸들러가
  원시값만 넘기고 스케일링은 항상 forward()가 담당하게 해서 둘이 어긋날 여지를 없앰.
- **합성 데이터를 일부러 완벽히 분리되지 않게 설계**: 처음엔 이상 패턴을 강하게(배율
  10~20배) 잡았으나, 그러면 단건 규칙 하나로도 다 잡혀서 "시퀀스를 볼 필요가 있는가"를
  증명할 수 없다고 판단해 배율을 3~8배로 낮추고 정상 분포와 겹치게 조정.
- **학습 산출물(.pt/.mar)을 커밋하지 않기로 결정**: 데이터 생성부터 학습까지 seed
  고정으로 완전히 결정적이라 스크립트만 있으면 재현 가능. 대신 실제 실행 결과 수치를
  이 세션 로그에 실측값으로 남김(아래 "완료 후 확인 방법").
- **CP2/CP3와의 인터페이스 간극을 이번 세션에서 메우지 않기로 결정**: 이 모델은
  "계좌의 최근 거래 각각"을 요구하는데, 현재 CP2/CP3는 "집계 결과 1개"만 저장하도록
  설계돼 있다(ARCHITECTURE.md 3번). `ai/pytorch-sequence-model` 브랜치는 "가짜 데이터로
  병렬 진행 가능"이 전제라 이 간극을 여기서 억지로 메우지 않고, `ai/README.md`에
  옵션(CP2 계약 확장 vs Redis 별도 리스트)만 남겨 다음 백엔드 세션의 논의거리로
  넘겼다. 트레이드오프: 지금은 `backend/model-client`가 이 모델을 실제로 호출하려면
  먼저 이 간극부터 해결해야 하는 순서 의존성이 생김.

## 막혔던 문제와 해결 방법

- **Windows 콘솔(cp949) 유니코드 인코딩 에러**: `evaluate.py`의 출력 문자열에 있던
  em dash(—, U+2014)가 cp949로 인코딩이 안 돼 `UnicodeEncodeError`로 죽음. 해당 문자를
  일반 하이픈(-)으로 바꿔서 해결. (근본적으로는 `PYTHONIOENCODING=utf-8`을 설정해도
  회피 가능하다는 것도 확인함.)
- **TorchServe 설치가 될 거라 예상 못 함**: Python 3.14가 최신이라 torchserve/
  torch-model-archiver가 설치조차 안 될 거라 예상하고 README에 "다음 세션 TODO"로
  먼저 적어뒀는데, 실제로 `pip install torchserve torch-model-archiver`가 그냥 성공함.
  다만 `from ts.torch_handler.base_handler import BaseHandler`에서
  `ModuleNotFoundError: No module named 'yaml'`이 남 — torchserve 0.12.0 패키지가
  pyyaml을 의존성으로 명시하지 않은 게 원인. `pip install pyyaml`로 해결하고
  requirements.txt에 명시적으로 추가함. 예상과 실측이 다를 수 있으니 "될 것 같지
  않다"고 적기 전에 실제로 시도해봐야 한다는 걸 다시 확인한 사례.
- **TorchServe가 handler.py의 절대 임포트(`from pytorch_sequence_model.config import
  ...`)를 못 찾음**: .mar 아카이브에는 handler.py 파일 자체만 들어가고 패키지 구조는
  안 들어가서, torchserve 프로세스의 PYTHONPATH에 `ai/` 디렉터리를 추가해줘야 config.py를
  찾을 수 있었다. 로컬 검증은 이렇게 해결했지만, 실제 운영 배포 방식(handler
  자기완결화 vs `--extra-files`)은 다음 세션 TODO로 남김.

## 완료 후 확인 방법 (실제로 수행함)

1. `python -m unittest discover -s tests` — 9개 단위 테스트 전부 통과.
2. `python -m pytorch_sequence_model.train --n-accounts 20000 --epochs 15` 실행 —
   `val_auc`가 0.9940(1 epoch) -> 0.9993(15 epoch)까지 단조 개선, 정상 수렴 확인.
3. `python -m pytorch_sequence_model.evaluate` 실행 — 테스트셋 3,000건(사기 481건,
   16.0%) 기준:
   - 시퀀스 모델(LSTM): AUC 0.9996, Precision 0.9711, Recall 0.9792, F1 0.9752
   - 베이스라인(마지막 거래 1건, 로지스틱 회귀): AUC 0.9235, Precision 0.6027,
     Recall 0.7443, F1 0.6660
   - AUC 차이(시퀀스 - 베이스라인): **+0.0761**, F1 차이: **+0.31** — 시퀀스 맥락을
     보는 것이 실제로 탐지력을 크게 높인다는 걸 합성 데이터 기준으로 정량 확인.
     (docs/PERFORMANCE_MEASUREMENT.md "개선 전/후 비교 방법"의 정확도/탐지력 관점 충족)
4. `python -m pytorch_sequence_model.serving.export`로 TorchScript export 후, 별도
   스크립트로 eager 모델과 traced 모델의 출력을 비교 — 서로 다른 길이(30, 10)의 배치에서
   `max abs diff = 0.0` 확인 (gather 기반 가변 길이 처리가 trace 후에도 정확히 동작).
5. `torch-model-archiver`로 `.mar` 생성 -> `torchserve --start`로 실제 기동 ->
   `/ping`이 `Healthy`, `/models`에 `fds-sequence-model` 등록 확인.
6. `/predictions/fds-sequence-model`에 실제 REST 요청 2건 전송:
   - 정상 패턴 시퀀스(금액배율 1 근처, 국가변경 없음) -> `fraudProbability: 0.0003`
   - 이상 패턴 시퀀스(마지막 거래에 금액 6.5배 급증 + gap 10초 + 국가변경 동시 발생) ->
     `fraudProbability: 1.0`
   두 경우 모두 기대한 방향으로 정확히 반응 — 학습→평가→export→아카이빙→서빙까지
   전체 파이프라인이 실제로 동작함을 end-to-end로 확인.
7. `torchserve --stop`으로 정상 종료 확인.

## 다음에 이어서 할 일

- **CP2/CP3 ↔ CP4 인터페이스 간극 해소**: 현재 파이프라인은 계좌별 "집계 결과 1개"만
  저장하는데 CP4는 "최근 N건 원본 시퀀스"가 필요함. `backend/kafka-streams-topology`
  또는 `backend/redis-feature-store` 쪽에서 어떻게 최근 이력을 보관할지 논의 필요
  (`ai/README.md` "백엔드 연동 시 남는 숙제" 절 참고).
- **`backend/model-client` 브랜치**: 이번 세션에서 확정한 REST 계약
  (`serving/handler.py` docstring)을 바탕으로 Spring Boot `ModelInferenceClient` +
  Resilience4j Circuit Breaker 구현 (BACKEND.md 1차 구현 범위에서 이미 제외 항목으로
  예고돼 있던 컴포넌트).
- **TorchServe 운영 배포 방식 정리**: handler.py가 `pytorch_sequence_model` 패키지를
  절대 임포트하는 현재 구조는 로컬 PYTHONPATH 트릭에 의존함 — Docker 이미지에
  패키지를 설치하거나 handler를 자기완결형으로 재작성하는 방식 결정 필요.
  Prometheus 연동(CP4 성능 측정 표: 추론 latency p50/p95/p99, 서킷브레이커 오픈
  발생률)도 이 배포 방식이 정해진 뒤에 이어서 진행.
- **실제 거래 패턴 기반 검증**: 지금은 합성 데이터만으로 학습/평가함. 실 서비스 로그나
  더 정교한 시뮬레이션(예: 실제 사기 사례 리포트 기반 패턴)으로 교체했을 때도 같은
  아키텍처가 유효한지 재검증 필요.
- **k6 부하 테스트 + Grafana 대시보드**: CP1~CP3와 같은 패턴으로 CP4 전용 k6 시나리오
  (`docs/PERFORMANCE_MEASUREMENT.md` CP4 "k6 시나리오 + 장애 주입") 및 대시보드 작성.
