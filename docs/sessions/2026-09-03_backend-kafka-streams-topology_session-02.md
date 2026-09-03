# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-03, backend/kafka-streams-topology, session-02

## 이번 세션에서 다룬 질문/요청

- "순서대로 해줘" — PR #2(CP2) 리뷰·머지 → CP2 패널 확장 → CP3 착수 순서로 진행 요청.
- 이 로그는 그중 "PR #2 리뷰·머지" 단계를 다룬다.

## 변경/결정된 내용

- `/code-review` 스킬로 PR #2(`backend/kafka-streams-topology` → `main`)를 medium 강도로 리뷰 —
  8개 병렬 파인더 에이전트 + `TopologyTestDriver`로 직접 재현한 결과, 실질적인 버그 다수 발견.
- 발견/수정 내역 (자세한 내용은 `docs/BACKEND.md` "코드 리뷰로 발견/수정한 문제" 참고):
  - `AccountActivityProcessor`: 역전된 이벤트가 (1) recentWindowCount를 영구히 부풀리고, (2)
    lastTransactionTimestamp를 오염시켜 이후 정상 이벤트의 gap까지 틀어지는 버그 → 윈도우 트리밍을
    `removeIf`로, last 필드 갱신을 "가장 최근 값만" 조건부로 바꿔 수정
  - occurredAt/amount가 null이면 NPE로 스트림 스레드 전체가 죽는 문제 → 프로세서에서 null 가드 추가
  - `amountRatio` 계산이 음수 누적 평균(환불 등)에서 부호 뒤집힌 값을 내던 문제 → 0 이하 가드
  - `AccountFeatureVector.recent5MinCount` → `recentWindowCount`로 개명 (설정 가능한 윈도우와 이름이
    안 맞던 문제)
  - CP1 대시보드의 컨슈머 랙 패널이 CP2가 삭제한 컨슈머 그룹(`fds-v2-partitioning-check`)을 계속
    쿼리하던 문제 → `fds-v2-streams-app`으로 교체
  - `FdsV2BackendApplicationTests`의 `auto-startup=false`가 실제로는 `KafkaAdmin`의 토픽 자동 생성을
    못 막던 문제 → `spring.kafka.admin.auto-create=false` + `bootstrap-servers=localhost:1`로
    이중 차단 (부수 효과로 테스트 시간도 45초 → 5.7초로 단축)
  - 로그(log.info)가 핫 파티션에서 병목이 될 수 있는 문제 → log.debug로 하향
  - 테스트가 프로덕션 토폴로지 배선을 따로 베껴 쓰던 문제 → `SequenceAggregationTopologyConfig`에
    static `buildTopology(...)` 메서드를 뽑아서 테스트가 재사용하도록 리팩터링
- `AccountActivityProcessorTest`에 회귀 테스트 3개 추가 (역전 이벤트의 gap 오염 방지, 역전 이벤트의
  카운트 영구 부풀림 방지, null 필드 방어) — 총 6개 테스트 전부 통과.

## 설계 의도 및 트레이드오프

- **transactionId 중복 제거는 이번에도 안 함**: 발견됐지만, State Store 스키마 확장(최근 본
  transactionId 집합 등)이 필요한 별도 크기의 작업이라 문서에 명시적으로 남기고 다음 개선 후보로
  미뤘다 — "고쳐야 하는데 안 고침"이 아니라 "고칠 방법과 이유를 알고 의도적으로 미룸"으로 남기는 게
  이 프로젝트의 관례.
- **역전 이벤트 자체의 값 정확도는 여전히 보장 안 함**: 이번 수정은 "역전된 이벤트가 상태를 영구히
  오염시키는 것"만 막았다. 그 역전된 이벤트 자신의 recentWindowCount/amountRatio가 "진짜 정확한 값"인지는
  여전히 보장하지 않는다 — 완벽한 이벤트 정렬(예: 워터마크 기반 재정렬)은 훨씬 큰 작업이라 범위 밖.
- **`spring.kafka.admin.auto-create=false`를 CP1 스모크 테스트 시점부터가 아니라 지금 발견**: CP1
  때는 컨슈머만 있어서 `KafkaAdmin`의 토픽 자동 생성이 우연히 문제를 안 일으켰거나 덜 눈에 띄었을
  수 있다. CP2에서 토픽이 2개(입력+출력)로 늘고 리뷰를 거치면서 근본 원인을 제대로 찾은 것 — 이런
  근본 수정은 나중에 CP1 쪽 테스트에도 같은 패턴이 필요한지 재검토할 만하다.

## 막혔던 문제와 해결 방법

- 없음 — 리뷰가 이미 각 문제를 `TopologyTestDriver`로 재현까지 해서 근거를 남겨준 덕분에, 수정
  방향을 고민할 필요 없이 바로 반영할 수 있었음.

## 확인 방법 (실제로 수행함)

```
./gradlew test   # 6개 테스트(기존 3 + 신규 회귀 테스트 3) 전부 통과, ~13초
```

- 리뷰가 지적한 버그를 각각 재현하는 회귀 테스트를 먼저 추가하고, 수정 전/후로 실패→통과가
  바뀌는 걸 확인하며 고쳤다 (역전 이벤트 2개 테스트, null 필드 방어 1개 테스트).
- `FdsV2BackendApplicationTests` 실행 시간이 45.3초 → 5.7초로 줄어든 것도 확인 — `auto-create=false`가
  실제로 불필요한 네트워크 재시도를 없앴다는 방증.
- 실제 브로커 e2e 재검증은 생략 — 이번 수정은 순수 로직 변경(+ 테스트 인프라)이고 Kafka 연결/토폴로지
  구조 자체는 안 바뀌어서, session-01에서 이미 한 e2e 검증과 이번 단위 테스트만으로 충분하다고 판단.

## 다음에 이어서 할 일

- 이 커밋을 push하고 PR #2를 main에 머지
- CP2 패널 확장(Kafka Streams 자체 지표 + RocksDB 지표를 Grafana에)
- CP3(`backend/redis-feature-store`) 착수
