# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-11, ai/ensemble-weight-tuning, session-02

## 이번 세션에서 다룬 질문/요청

- PR #15를 `/code-review` medium 강도로 리뷰 → 반영.

## 변경/결정된 내용

7개 findings 중 3개 자동 반영, 1개는 리뷰가 못 고친 것을 직접 마저 고침, 3개는 범위 밖으로
명시적 보류:

1. **(가장 중요, 자동 반영 안 됨 — 직접 수정) `CostWeights`의 비용 순서가 문서화한 의도와 반대**:
   docstring은 "fraud_allow > fraud_step_up > normal_block > normal_step_up"이라고 명시했는데,
   실제 기본값은 `fraud_step_up=4.0 < normal_block=6.0`으로 거꾸로였다 — session-01에서 보고한
   modelWeight=1.0/ruleWeight=0.0 결과 전체가 이 잘못된 비용함수로 최적화된 값이었다는 뜻.
   `fraud_step_up=8.0`(normal_block보다 크고 fraud_allow보다 작게)으로 정정하고 그리드서치를
   재실행 — 새 결과는 modelWeight=0.95/ruleWeight=0.05, low=0.25/high=0.45(기대 비용 0.0817,
   기존 하드코딩값 대비 약 34% 낮음). `application.yml`/javadoc/ARCHITECTURE.md/ai/README.md에
   있던 session-01 수치를 전부 이 값으로 교체했다.
2. **(자동 반영) `EnsembleFraudDecisionService`의 `@Value` 기본값이 옛 값(0.7/0.3/0.3/0.7)에
   머물러 있었음**: `application.yml`은 튜닝값으로 바꿨는데 Java 쪽 `@Value(...)`의 fallback
   리터럴은 안 바꿔서, `application.yml`이 로드 안 되는 환경(슬림 프로파일, 일부 테스트 컨텍스트)
   에서는 조용히 옛 값으로 되돌아가는 문제. `EnsembleFraudDecisionServiceTest`가 전부 명시적
   파라미터로 생성해서 이 드리프트가 CI에서 안 잡혔다. 네 기본값을 application.yml과 동일하게
   수정(이번엔 0.95/0.05/0.25/0.45).
3. **(자동 반영) `summarize()`가 `expected_cost()`의 비용 계산 로직을 복붙**: 그리드서치가
   최적화하는 비용과 사람이 보는 리포트의 비용이 나중에 조용히 갈라질 위험 — `summarize()`가
   `expected_cost()`를 직접 호출하도록 수정.
4. **(자동 반영) `expected_cost()`가 그리드서치 반복마다(~3,591회) `costs.as_table()`을 매번
   새로 생성**: `grid_search()`가 테이블을 한 번만 만들어 넘기도록 `table` 파라미터 추가(생략
   가능, 기존 직접 호출/테스트 코드와 호환).
5. **(보류) `rule_score()`가 null 입력(중립값 0.5) 분기를 재현하지 않음**: 지금은
   ruleWeight가 작아서(0.05) 영향이 제한적이지만, 나중에 규칙 비중을 높이면 검증 안 된 경로가
   된다 — 스크립트 구조를 안 바꾸고는 자연스러운 호출 지점이 없어서 이번엔 보류.
6. **(보류) `evaluate.py`와 `tune_ensemble.py`가 모델 스코어링 루프(DataLoader/no_grad/
   predict_proba)를 중복 구현**: 공유 헬퍼로 뽑으려면 이 PR 범위 밖인 `evaluate.py`를 건드려야
   해서 보류.
7. **(보류) 산정값이 `tune_ensemble.py` 최신 실행 결과와 계속 일치하는지 확인하는 안전장치
   없음**: CI에서 그리드서치를 재실행해 `application.yml`과 diff하는 건 인프라 작업이라 범위
   밖으로 남김.

## 설계 의도 및 트레이드오프

- **비용함수 버그를 "그냥 문서만 고치기"로 덮지 않고 실제로 재실행함**: 리뷰가 "이 버그를
  고치려면 torch 파이프라인을 다시 돌려야 하고 하위 문서 수치도 다 바꿔야 한다"며 자동 수정을
  포기했지만, session-01에서 이미 WSL 환경을 구축해둔 상태라 재실행 비용이 낮았다 — 그래서
  직접 재실행하고 하위 문서(4곳)를 전부 교체했다. "찾았지만 못 고친 채로 둔다"보다 "찾은 김에
  실제로 고친다"는 이 프로젝트의 기존 원칙(salting의 stale-shard 버그와 동일)을 따랐다.
- **session-01 로그 자체는 수정하지 않음**: session-01은 "그 시점에 실제로 일어난 일"의
  기록이라 사후에 고치지 않고, 이 session-02가 무엇이 왜 바뀌었는지 별도로 기록한다 — 나중에
  "왜 숫자가 두 버전인가"를 물으면 이 로그가 그 자체로 답이 된다.

## 막혔던 문제와 해결 방법

- 없음 — WSL 환경이 이미 구축돼 있어서 CostWeights 값 하나 고치고 재실행하는 데 막힌 부분
  없었다.

## 확인 방법 (실제로 수행함)

```
# WSL Ubuntu 24.04, /root/fds-ai-venv
python -m unittest discover -s tests    # 22개 전부 통과(값 변경의 영향 없음 — 테스트는
                                         # 정확한 상대 순서가 아니라 "어느 한쪽에 의존해야
                                         # 한다"는 성질만 검증하도록 짜여 있었음)
python -m pytorch_sequence_model.tune_ensemble   # 정정된 CostWeights로 재실행

# 저장소 루트(Windows)
./gradlew compileJava   # EnsembleFraudDecisionService 기본값 수정 후 컴파일 확인
```

정정된 test set(3,000건, 사기 481건) 결과:

| | model:rule 가중치 | low/high | 기대 비용 | 사기 완전누출율 | 사기 완전차단율 | 정상거래 마찰율 |
|---|---|---|---|---|---|---|
| 기존 하드코딩값 | 0.7 : 0.3 | 0.30/0.70 | 0.1240 | 1.04% | 93.35% | 1.23% |
| 그리드서치 산정값(정정 후) | 0.95 : 0.05 | 0.25/0.45 | 0.0817 | 1.25% | 98.34% | 0.95% |

(비용 절대값이 session-01과 달라진 건 CostWeights 자체가 바뀌었기 때문 — 두 표는 서로 다른
비용함수 기준이라 직접 비교하면 안 된다. session-02 표가 최종/올바른 버전이다.)

세부 카운트(정정 후, test set): `{normal_allow: 2495, normal_step_up_auth: 7, normal_block: 17,
fraud_allow: 6, fraud_step_up_auth: 2, fraud_block: 473}`

## 다음에 이어서 할 일

- 머지.
- 보류한 3개 findings(null 규칙점수 재현, evaluate.py/tune_ensemble.py 스코어링 루프 중복,
  산정값-스크립트 드리프트 감지)는 우선순위가 낮아 별도 이슈로만 남겨두고 지금 당장 처리하지
  않음.
- session-01의 "다음에 이어서 할 일"(실제 라벨 데이터로 CostWeights 재추정, WSL 안내 문서화)은
  그대로 유효.
