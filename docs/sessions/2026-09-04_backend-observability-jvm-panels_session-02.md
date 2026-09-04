# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, backend/observability-jvm-panels, session-02

## 이번 세션에서 다룬 질문/요청

- "PR 처리해줘" — 마지막으로 남아있던 PR #3(JVM/CPU 패널)을 다른 PR들과 동일한 기준
  (`/code-review` → 반영 → 실측 재검증 → merge)으로 처리.

## 변경/결정된 내용

- `/code-review`로 PR #3을 medium 강도로 리뷰 — 1개 발견, 실측으로 확인 후 반영:
  - **JVM 힙 max 패널이 G1GC의 -1(무제한) 풀을 합계에 포함**: `sum(jvm_memory_max_bytes{area="heap"})`
    가 "G1 Eden Space"/"G1 Survivor Space" 풀의 `max=-1`(G1은 리전 기반이라 이 두 풀은 고정
    최대치가 없음)까지 그대로 더해서, 실제 `-Xmx`보다 낮게 나온다. 실측: 필터 없이 8459911166,
    `> 0` 필터 적용 후 8459911168(= G1 Old Gen 단독 값, 실제 `-Xmx`와 일치). 지금 차이는 2바이트뿐
    이라 무해하지만, GC 종류나 풀 구성이 다른 환경에서는 더 크게 어긋날 수 있어 원칙대로 고쳤다.
  - 수정: `sum(jvm_memory_max_bytes{area="heap"} > 0)`로 -1 풀 제외.

## 설계 의도 및 트레이드오프

- **작은 오차라도 근본 원인대로 고침**: 지금 겪은 오차는 2바이트라 사실상 무해하지만, "음수
  sentinel 값이 합계 쿼리에 섞이는" 패턴 자체가 이번 세션에서 CP2/CP3 관측 확장 때도 여러 번
  문제를 일으켰던 것과 같은 종류라(예: restore-remaining-records-total의 -1), 크기와 무관하게
  원칙(0 이하 값은 "값 없음"으로 취급해 걸러낸다)대로 고쳐두는 게 다음에 같은 함정에 또 걸리지
  않는 길이라고 판단했다.

## 확인 방법 (실제로 수행함)

```
SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 \
  FDS_REDIS_PORT=16379 ./gradlew bootRun
```

- `/actuator/prometheus`에서 `jvm_memory_max_bytes{area="heap"}` per-pool 값을 직접 확인 —
  `G1 Eden Space`/`G1 Survivor Space`가 `-1.0`, `G1 Old Gen`만 실제 값(`8459911168`).
- Prometheus에서 수정 전/후 쿼리를 나란히 실행해서 `8459911166` → `8459911168`로 바뀌는 것 확인
  (JSON 유효성도 재검증).

## 다음에 이어서 할 일

- PR #3 merge.
- 열려있는 PR이 모두 처리되면, CP4(PyTorch 모델 서빙) 착수 여부 논의.
