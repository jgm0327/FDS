# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-10, backend/decision-ensemble, session-02

## 이번 세션에서 다룬 질문/요청

- PR #11(CP5)을 `/code-review` medium 강도로 리뷰(8개 관점 + 검증) → 반영.

## 변경/결정된 내용

`/code-review`가 10개 findings를 냈고(전부 CONFIRMED/PLAUSIBLE, 반박된 것 없음), 그중 정확성/설계
원칙 위반에 해당하는 3개를 수정했다:

1. **`FraudDecisionEventListener`를 동기 → 비동기로 전환**: `DecisionAsyncConfig`(`@EnableAsync` +
   전용 `ThreadPoolTaskExecutor` "fraudDecisionExecutor") 신규 추가, 리스너 메서드에
   `@Async("fraudDecisionExecutor")` 부여. 기존엔 CP3의 Kafka 컨슈머 스레드에서 매 거래마다
   TorchServe 호출(최대 timeout-ms)이 동기로 걸려서, CLAUDE.md 설계 원칙 4번("Spring Boot가
   비동기 호출")을 어기고 컨슈머 랙/리밸런싱 위험을 낳는다는 지적.
2. **`EnsembleFraudDecisionService.toAction()`에 NaN 가드 추가**: combinedScore가 NaN이면
   `<` 비교가 전부 false가 되어 조용히 BLOCK으로 떨어지던 문제 → NaN을 명시적으로 감지해서
   STEP_UP_AUTH(판단 불가는 완충 처리)로 분기. 회귀 테스트 1건 추가.
3. **`AccountFeatureStoreSinkListener`의 `eventPublisher.publishEvent(...)` 호출을 try/catch로
   방어**: 여기서 예외가 새면 이미 성공한 Redis 쓰기가 있는 레코드가 재시도(중복 RPUSH)될 위험이
   있다는 지적.

## 설계 의도 및 트레이드오프

- **비동기 전환이 레이스 컨디션 방지 전제를 깨지 않는 이유**: session-01에서 "CP3가 Redis 쓰기를
  마친 이후에 이벤트를 발행하면 레이스가 안 생긴다"고 설계했는데, 이건 "이벤트가 언제 발행되는가"의
  문제지 "그 이벤트를 어떤 스레드가 처리하는가"의 문제가 아니다. 처리 스레드를 전용 풀로 옮겨도
  발행 시점(RPUSH 완료 후) 자체는 그대로라 안전하다 — 리뷰가 지적한 "컨슈머 스레드 블로킹" 문제와
  session-01의 "레이스 방지" 설계는 서로 다른 축이라 둘 다 만족시킬 수 있었다.
- **NaN → BLOCK이 아니라 STEP_UP_AUTH를 택한 이유**: NaN은 "확인된 고위험"이 아니라 "판단 불가"다.
  3단계 액션의 원래 취지가 "애매한 구간을 완충"하는 것이므로, 판단이 아예 안 되는 상황도 같은
  방식(추가인증)으로 완충하는 게 일관성 있다고 판단했다.
- **10개 중 3개만 수정하고 나머지는 보류**: 아래 "보류한 항목" 참고. 전부 고쳐야 할 이유는
  있지만, 일부는 이번에 손대기엔 리스크가 더 큰 아키텍처 변경(예: 이중 Redis 읽기 제거)이라
  범위를 좁혔다 — 이 프로젝트가 지금까지 일관되게 해온 "고칠 이유를 알고 의도적으로 미룸" 방식.

## 보류한 항목 (이유 포함)

- **규칙/모델 이중 Redis 읽기(레이스 가능성)**: `FraudDecisionController`가 latestStep을 위해
  한 번, `ModelInferenceClient.predict()` 내부에서 시퀀스 전체를 위해 또 한 번 — 두 번 다른
  시점에 Redis를 읽는다. 고치려면 "한 번 읽어서 양쪽에 전달"하는 시그니처 변경이 필요한데, 이건
  이 컨트롤러(측정 전용, 기본 비활성)보다는 자동 트리거 경로(`FraudDecisionEventListener`가
  이미 최신 `featureJson`을 이벤트로 들고 있음)의 설계와 얽혀있어 더 큰 리팩터링이 필요하다고
  판단, 이번엔 보류.
- **`RuleBasedFallbackScorer.score(null)==0.5`를 애매한 구간의 규칙 점수로 재사용하는 것**: CP4의
  "폴백 전용" 스코어러를 CP5가 "정상 경로"에서도 쓰는 의미론적 재사용 — 동작은 하지만 이름과
  용도가 어긋난다. RuleEngine 전용 점수 산출 로직으로 분리하는 게 맞지만, CP4/CP5 스코어러 통합
  여부 자체가 session-01에서 이미 "backend/model-client-concurrency-fix 머지 후 재검토"로
  남긴 별도 논의라 이번 세션에서 같이 건드리지 않았다.
- **앙상블 가중치/임계값 합/순서 검증 부재, 화이트리스트 CSV 파싱을 StringUtils로 교체, 중복된
  "latest step 추출" 로직, 판정 로그의 구조화 로깅 컨벤션, ObjectMapper 수동 생성**: 전부
  낮은 심각도의 정리성 지적이라 이번엔 보류. 특히 ObjectMapper는 이 PR만의 문제가 아니라
  `AccountRecentSequenceReader`/`TorchServeModelInferenceClient`(CP4)에도 이미 있던 반복
  패턴이라, 이 PR 하나만 고치면 오히려 일관성이 깨진다 — 손대려면 레포 전체 차원의 별도 작업.

## 확인 방법 (실제로 수행함)

```
./gradlew test   # 신규 회귀 테스트 1건 포함 전체 통과
```

## 다음에 이어서 할 일

- session-01의 "다음에 이어서 할 일" 그대로 유효 (실제 Kafka/Redis e2e 검증, CP5 관측 확장,
  docs/BACKEND.md CP4/CP5 스펙 소급 기록).
- 위 "보류한 항목" 중 이중 Redis 읽기는 CP5 관측 확장이나 다음 리팩터링 세션에서 재검토.
