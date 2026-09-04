# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, backend/sequence-window-feature-store, session-01

## 이번 세션에서 다룬 질문/요청

- (ai/pytorch-sequence-model 세션에서 이어짐) "CP2/CP3 ↔ CP4 인터페이스 간극 논의" — CP4
  시퀀스 모델이 요구하는 "계좌의 최근 거래 시퀀스"를 CP2/CP3가 어떻게 공급할지 설계하고
  구현까지 진행

## 변경/결정된 내용

- **핵심 발견**: CP2(`account-feature-updates`)는 이미 계좌당 거래 1건마다 메시지 1개를
  발행하고, 그 메시지의 `amountRatio`/`lastTxGapSec`/`countryChanged`는 전부 "그 거래
  자체"의 스텝 값이다(BACKEND.md CP2 검증 기록 재확인: count 1→2→3, ratio 1.0→3.0→0.25 등
  이미 스텝별로 다름). 즉 CP4가 필요로 하는 "원본 시퀀스"는 이미 Kafka 스트림 위에
  존재하고, CP3가 "최신 값만 덮어쓰기"로 그 이력을 버리고 있을 뿐이었다.
- **`AccountFeatureVector`(CP2)**: `merchantCategory` 필드 1개 추가. 이미
  `TransactionEvent`에 있는 값을 그대로 통과시킴 — 핵심 집계 로직(`AccountActivityState`,
  `AccountActivityProcessor`의 윈도우/평균/경과시간 계산)은 전혀 안 건드림.
- **`AccountFeatureStoreSinkListener`(CP3)**: 기존 스냅샷 `SET`은 그대로 두고, 같은 JSON을
  `feature:account:{id}:recent` Redis LIST에 `RPUSH` + `LTRIM`(최근 `recentWindowSize`건만
  유지) + `EXPIRE`(스냅샷과 같은 TTL, 매 거래마다 리셋).
  - JSON을 역직렬화하지 않고 그대로 RPUSH — CP3의 기존 원칙("CP2 도메인 클래스에 의존하지
    않고 JSON 형태만 계약으로 삼는다")을 그대로 유지.
- **`FeatureStoreKeyBuilder`**: `recentKey(accountId)` 메서드 추가 (`feature:account:{id}:recent`).
- **`application.yml`**: `fds.feature-store.recent-window-size`(기본 30) 추가 — ai/README.md에
  이미 문서화했던 "CP4의 MAX_SEQ_LEN과 반드시 같아야 한다"는 제약을 여기도 명시.
- **테스트**: `AccountActivityProcessorTest`에 merchantCategory pass-through 검증 2건 추가,
  `AccountFeatureStoreSinkListenerTest`에 RPUSH/TRIM/TTL 검증 1건 추가 + 기존 테스트에
  `opsForList()` 목 스텁 보강.

## 설계 의도 및 트레이드오프

- **검토한 3가지 옵션 중 "CP3에 Redis LIST 추가"를 선택**(다른 두 옵션: CP2 State Store에
  스텝 이력을 직접 저장 / Spring Boot 쪽에서 자체 버퍼링). CP2 State Store에 쌓는 안은 이미
  검증/머지된 핵심 로직에 손을 대야 하고 changelog 용량도 계좌당 N배 늘어나는 반면, CP3
  확장안은 이미 있는 컴포넌트에 한 줄 추가하는 수준이라 리스크가 훨씬 작다. Spring Boot
  자체 버퍼링은 재시작 시 유실되어 State Store/Redis가 이미 제공하는 영속성을 재발명하는
  격이라 제외.
- **JSON을 역직렬화하지 않고 그대로 RPUSH**: CP3가 CP2의 자바 타입을 몰라도 되게 하려는
  기존 설계 원칙(병렬 worktree 원칙)을 이 확장에도 똑같이 적용. CP2가 필드를 더 추가해도
  CP3(이 리스너)는 재컴파일 없이 그대로 통과시킨다.
- **recentWindowSize를 별도 config로 뺌**: ai/ 쪽 MAX_SEQ_LEN과 서로 다른 언어/레포 경계에
  있어 컴파일 타임에 맞출 방법이 없다 — 두 값이 어긋나면 서빙 시점에 모델이 기대하는
  컨텍스트 길이와 실제 시퀀스 길이가 달라지는 조용한 버그가 된다. 둘 다 "여기서 정의하고
  서로를 참조하는 주석"으로만 연결해뒀다 — 완전 자동화(예: 공유 스키마 레지스트리)는 이번
  범위 밖.
- **key-prefix는 config인데 ":recent" 접미사는 상수로 고정**: key-prefix는 운영 환경별 네임
  스페이스라 바뀔 수 있지만, ":recent"는 이 코드베이스 안에서만 의미를 갖는 구분자라 설정화
  실익이 없다고 판단(과도한 설정화 방지).

## 막혔던 문제와 해결 방법

- **Mockito strict stubbing으로 인한 NPE**: 새 RPUSH/TRIM 로직을 추가한 뒤 처음 테스트를
  돌렸을 때 `계좌ID를_키로_JSON_원문을_TTL과_함께_저장한다` 테스트가
  `NullPointerException`으로 실패했다 — `opsForValue()`만 스텁하고 `opsForList()`는
  스텁하지 않아서, 목 객체가 기본값(null)을 반환하는 `ListOperations`에 `.rightPush(...)`를
  호출한 게 원인. 해당 테스트에도 `when(redisTemplate.opsForList()).thenReturn(listOperations)`를
  추가해서 해결 — 재발 방지 겸, "리스너가 실제로 SET과 RPUSH를 둘 다 호출한다"는 동작 자체를
  테스트가 더 정확히 반영하게 됐다.
- **docker-compose 컨테이너 이름 충돌**: 이 worktree에서 `docker compose up`을 실행했더니
  `fds-v2-kafka` 등 컨테이너 이름이 이미 사용 중이라는 에러가 났다 — 확인해보니 다른
  worktree/세션에서 띄운 동일한 스택(Kafka/Redis/Prometheus/Grafana, 포트 19092/16379 등)이
  이미 정상 기동 중이었다. 모든 worktree가 같은 docker-compose.yml(고정 컨테이너 이름)을
  공유하므로 사실상 "로컬 인프라는 전역에 하나만" 뜬다는 뜻 — 새로 띄우지 않고 기존
  스택에 그대로 붙어서 e2e 검증을 진행했다.

## 완료 후 확인 방법 (실제로 수행함)

1. `./gradlew test` — 전체 테스트(신규 3건 포함) 통과. (첫 실행에서 위 Mockito NPE로 1건
   실패 → 수정 후 재실행 전부 통과)
2. 실제 브로커+Redis(이미 떠 있던 CP1~CP3 docker-compose 스택, 포트 19092/16379)로 앱을
   기동해서 e2e 검증: `acc-seqwindow-e2e` 계좌로 서로 다른 금액/가맹점 카테고리의 거래 3건
   연속 발행 →
   - 스냅샷 키(`feature:account:acc-seqwindow-e2e`): 최신 값(`recentWindowCount:3,
     merchantCategory:GROCERY_3`)만 있음 — 기존 CP3 스펙 그대로 유지됨을 확인.
   - 신규 리스트 키(`feature:account:acc-seqwindow-e2e:recent`): `LRANGE 0 -1`로 3건 전부
     순서대로(`GROCERY_1`→`GROCERY_2`→`GROCERY_3`) 조회됨, 각 항목의 amountRatio/
     lastTxGapSec/countryChanged가 스텝별로 다른 값 확인.
   - `TTL`: 1787초(~30분) 확인 — 스냅샷과 동일한 TTL 정책 적용됨.
   - `LLEN`: 3 — append(덮어쓰기 아님) 스펙 확인.
   검증 후 테스트 키는 정리(DEL).

## 다음에 이어서 할 일

- **`backend/model-client` 브랜치**: `feature:account:{id}:recent`를 `LRANGE`로 읽어
  `lastTxGapSec→gapSec` 등 필드명만 맞춰 CP4 TorchServe REST 요청의 `transactions` 배열로
  변환하는 어댑터 구현 + Resilience4j Circuit Breaker.
- **`recentWindowSize`(백엔드) ↔ `MAX_SEQ_LEN`(ai/)이 어긋나는 것을 막는 안전장치**: 지금은
  주석으로만 연결돼 있다 — CI에서 두 값을 비교하는 간단한 체크 등을 고려해볼 것.
- **e2e 검증에서 다루지 않은 것**: `recentWindowSize`(30건)를 초과하는 장기 계좌에 대한
  LTRIM 동작 실측(이번엔 3건만 발행) — 다음에 30건 넘게 발행해서 앞쪽이 실제로 잘려나가는지
  확인 필요.
