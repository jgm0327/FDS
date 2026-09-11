# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-11, ai/ensemble-weight-tuning, session-01

## 이번 세션에서 다룬 질문/요청

- (salting PR 머지 이후) "다음 진행할 작업은 뭐야?" — ARCHITECTURE.md TODO 중 "앙상블 가중치/임계값
  초기값 산정 방법"을 다음 작업으로 추천하고 진행 승인받음.
- `EnsembleFraudDecisionService`의 modelWeight/ruleWeight(0.7/0.3)와 low/high 임계값(0.3/0.7)이
  정성적 직관으로 하드코딩되어 있던 것을, CP4 학습에 쓴 합성 라벨 데이터로 실제 검증/산정.

## 변경/결정된 내용

- `ai/pytorch_sequence_model/tune_ensemble.py` 신규: val/test set 재현(train.py와 동일 seed/split)
  → 각 시퀀스의 (라벨, 모델확률, 규칙점수) 튜플 생성 → model_weight/low/high 그리드서치(비용
  함수 최소화) → 선택된 조합을 test set에 적용해 기존 하드코딩값과 나란히 보고.
  - `rule_score()`는 `com.fdsv2.modelclient.RuleBasedFallbackScorer#score()`를 그대로 Python으로
    재구현(0.1/0.6/0.9, 임계값 5.0) — 두 구현이 어긋나면 그리드서치가 실제 백엔드와 다른 값을
    추천하게 되므로 반드시 동일하게 유지해야 한다.
  - `CostWeights`: 액션(ALLOW/STEP_UP_AUTH/BLOCK) x 라벨(정상/사기) 조합별 비용 가정
    (fraud_allow=20, fraud_step_up=4, fraud_block=0, normal_allow=0, normal_step_up=1,
    normal_block=6). 실제 비즈니스 비용 데이터가 없어 가정치이며, 상대적 크기 순서(사기 완전
    누출이 제일 나쁨 > 정상거래 차단 > 사기 추가인증만 > 정상거래 추가인증 > 완전 정답)만
    근거가 있다.
  - 그리드서치는 정확도 지표(F1 등)가 아니라 이 비용 함수를 최소화하는 조합을 찾는다 — 3단계
    액션 구조에서는 "사기를 추가인증으로 거른 것"과 "완전히 놓친 것"을 이진 지표로 구분할 수
    없기 때문.
- `ai/tests/test_tune_ensemble.py` 신규: `rule_score()`가 Java 원본과 값이 일치하는지(경계값
  포함), `actions_for()`의 경계 규칙("<" 비교), `expected_cost()`의 기본 계산, `grid_search()`가
  "한쪽 신호만 변별력이 있으면 그 신호에 의존하는 조합을 찾아낸다"는 성질(정확한 최소 가중치
  값은 그리드 해상도에 좌우되므로 단언하지 않음)을 검증 — 총 9개 테스트, 기존 13개(dataset/
  model/synthetic)와 합쳐 22개 전부 통과.
- `src/main/resources/application.yml`: `fds.decision.ensemble.*` 기본값을 0.7/0.3, 0.3/0.7 →
  1.0/0.0, 0.20/0.75로 변경(아래 실측 결과 근거).
- `EnsembleFraudDecisionService` 클래스 javadoc, `docs/ARCHITECTURE.md` 5번(판정 및 대응),
  `ai/README.md`에 방법론/실측 결과 반영. `docs/ARCHITECTURE.md` TODO 체크.

## 설계 의도 및 트레이드오프

- **val set에서만 그리드서치, test set은 최종 보고 전용**: `evaluate.py`가 이미 모델 자체를
  test set으로 평가하는데, 앙상블 파라미터까지 test set에 맞춰 고르면 "test set에 대한 이중
  최적화"가 되어 보고되는 성능이 실제보다 낙관적으로 부풀려진다. train/val/test 3분할 원칙을
  그대로 지켰다.
- **정확도 지표 대신 비용 함수**: 이 시스템은 이진 판정이 아니라 3단계 액션이라, precision/
  recall/F1만으로는 "사기를 추가인증으로 거른 것"(부분적으로는 안전)과 "완전히 놓친 것"(가장
  위험)을 같은 것으로 취급하게 된다. 비용 함수는 이 둘을 구분해서 반영할 수 있다 — 대신
  비용 가정치 자체가 주관적이라는 한계를 명시적으로 문서화했다(salting 세션의 "정확한 것과
  근사인 것을 구분" 원칙과 같은 태도).
- **규칙weight=0을 그대로 채택**: 그리드서치 결과를 임의로 완화(예: 최소 0.1은 규칙에 배정)하지
  않고 실측값을 그대로 반영했다 — 근거는 `ModelInferenceClient`의 서킷브레이커 폴백 경로가
  이미 `RuleBasedFallbackScorer`의 출력을 "모델확률" 자리에 대입하는 구조라, ruleWeight=0이라도
  규칙 신호가 시스템에서 완전히 사라지는 게 아니기 때문이다. 이 안전장치가 없었다면 "정확도
  때문에 규칙을 아예 안 쓴다"는 결정은 훨씬 위험했을 것이다.

## 막혔던 문제와 해결 방법

- **(가장 큰 문제) Windows 11 스마트 앱 컨트롤(Smart App Control)이 강제(enforcement) 모드로
  torch/scipy의 네이티브 DLL 로딩 자체를 차단**: `import torch` 시
  "애플리케이션 제어 정책에서 이 파일을 차단했습니다" 에러. `Get-CimInstance ... Win32_DeviceGuard`로
  확인한 결과 `VerifiedAndReputablePolicyState=1`(시행 모드) — 이 기능은 한번 켜지면 Windows
  재설치 없이는 끌 수 없게 설계되어 있어 우회 시도 자체를 하지 않기로 함. 대안으로 WSL(Ubuntu
  24.04)을 설치해서 그 안에 별도 Python venv(`/root/fds-ai-venv`)를 만들고 CPU 전용 torch를
  설치 — WSL은 완전히 별도의 리눅스 커널(별도 실행 환경)이라 Windows의 코드 무결성 정책이
  적용되지 않는다. `wsl --install -d Ubuntu-24.04 --no-launch` + `-u root`로 실행해 대화형
  최초 설정(사용자 생성 프롬프트)도 건너뜀.
- **`summarize()`의 `KeyError: 'normal_step_up'`**: counts 딕셔너리 키를
  `f"{label}_{ACTIONS[act].lower()}"`로 만들면서 `ACTIONS = ("ALLOW", "STEP_UP_AUTH", "BLOCK")`의
  `STEP_UP_AUTH.lower()`가 `"step_up_auth"`인데, 뒤에서 `"normal_step_up"`으로 잘못 참조 —
  실행하자마자 바로 발견/수정.
- **초기 `GridSearchTest`의 결정론 오해**: "규칙점수가 상수(변별력 없음)면 model_weight를
  아무리 작게 섞어도(w>0) 무조건 완벽히 분리될 것"이라 가정하고 `model_weight==1.0`을
  단언했는데, 실제로는 threshold_step=0.05 그리드 해상도 때문에 w=0.1에서 먼저 비용 0을
  달성해 테스트가 실패했다. 근본 원인은 테스트의 가정이 이산 그리드의 해상도를 고려하지 않은
  것 — 정확한 최소 가중치 값 대신 "어느 쪽으로든 치우쳐야 한다"는 성질만 단언하도록 테스트를
  고쳐서 그리드 해상도를 바꿔도 깨지지 않게 만들었다.

## 확인 방법 (실제로 수행함)

```
# WSL Ubuntu 24.04, /root/fds-ai-venv
python -m unittest discover -s tests   # 22개 전부 통과 (기존 13개 + 신규 9개)
python -m pytorch_sequence_model.train        # 체크포인트 재생성
python -m pytorch_sequence_model.tune_ensemble

# 저장소 루트(Windows)
./gradlew test   # application.yml 기본값 변경 후에도 전체 통과 — 기존 테스트가 전부
                 # 명시적 파라미터로 서비스를 생성해서 기본값 변경의 영향을 받지 않음을 확인
```

test set(3,000건, 사기 481건, 16.0%) 실측 결과:

| | model:rule 가중치 | low/high | 기대 비용 | 사기 완전누출율 | 사기 완전차단율 | 정상거래 마찰율 |
|---|---|---|---|---|---|---|
| 기존 하드코딩값 | 0.7 : 0.3 | 0.30/0.70 | 0.0880 | 1.04% | 93.35% | 1.23% |
| 그리드서치 산정값 | 1.0 : 0.0 | 0.20/0.75 | 0.0633 | 0.83% | 96.67% | 1.27% |

세부 카운트(test set):
- 기존값: `{normal_allow: 2488, normal_step_up_auth: 26, normal_block: 5, fraud_allow: 5, fraud_step_up_auth: 27, fraud_block: 449}`
- 산정값: `{normal_allow: 2487, normal_step_up_auth: 26, normal_block: 6, fraud_allow: 4, fraud_step_up_auth: 12, fraud_block: 465}`

## 다음에 이어서 할 일

- 코드 리뷰 후 머지.
- 실제 라벨 데이터가 쌓이면 `CostWeights` 가정치 자체를 실측 비용으로 재추정하고 그리드서치를
  재실행 — 지금 결과는 합성 데이터 분포 + 가정한 비용표 위에서만 유효하다.
- WSL 환경(`/root/fds-ai-venv`)은 이번 세션에서 새로 만든 것이라 앞으로 CP4/AI 관련 세션에서
  계속 재사용 가능 — `docs/WORKTREE_SETUP.md`나 `ai/README.md`에 "Windows에서 Smart App
  Control이 torch를 막으면 WSL에서 작업" 안내를 추가하는 것도 고려.
- ARCHITECTURE.md 나머지 TODO(TorchServe gRPC 결정, 재학습 파이프라인 설계, CP1~CP7 재배치)는
  여전히 유효.
