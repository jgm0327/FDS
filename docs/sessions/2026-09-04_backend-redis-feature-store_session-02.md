# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, backend/redis-feature-store, session-02

## 이번 세션에서 다룬 질문/요청

- "순서대로 계속 해줘" — PR #5(CP3)를 `/code-review`로 리뷰 후 반영.

## 변경/결정된 내용

- `/code-review`로 PR #5를 medium 강도로 리뷰 — 2개 항목 발견, 전부 반영:
  1. **에러 처리/재시도 전무**: Redis 장애 시 그 사이 도착한 레코드가 기본 동작(사실상 재시도
     없이 offset 커밋)으로 조용히 유실되던 문제 → `FeatureStoreKafkaConfig`에 `DefaultErrorHandler`
     빈 추가(`FixedBackOff(1000ms, 3회)`). Spring Boot가 이 빈을 기본 리스너 컨테이너 팩토리에
     자동으로 연결해준다는 걸 `ConcurrentKafkaListenerContainerFactoryConfigurer` 바이트코드로
     확인. 재시도 소진 시 ERROR 로그를 남기고 다음 레코드로 넘어감 (DLT까지는 범위 밖, 문서화).
  2. **32개 파티션인데 concurrency 미지정**: 컨슈머 스레드 1개가 전체를 직렬 처리하던 문제 →
     `@KafkaListener(concurrency = "${fds.feature-store.consumer-concurrency:4}")` 추가. 실제
     기동 로그로 4개 스레드가 8개씩 나눠 갖는 것 확인.
- `AccountFeatureStoreSinkListenerTest`에 회귀 테스트 1개 추가 — Redis 쓰기 실패 시 리스너가
  예외를 삼키지 않고 그대로 전파하는지 검증 (에러 핸들러가 재시도할 기회를 얻으려면 필수).

## 설계 의도 및 트레이드오프

- **DLT(Dead Letter Topic)까지는 안 만듦**: 재시도(1초 간격 3회) 후에도 실패하면 로그만 남기고
  넘어간다. 완전한 무손실을 보장하려면 DLT + 재처리 파이프라인이 필요하지만, 이번 범위는 "조용히
  사라지는 것"을 "로그로 드러나는 것"으로 바꾸는 데까지만 — 다음 개선 후보로 문서화.
- **concurrency 기본값 4 (32 아님)**: CP1의 "넉넉하게 시작 → 실측 후 조정" 원칙을 그대로 따름.
  인스턴스 하나에서 32개 스레드를 다 쓰는 건 로컬 개발 환경 기준 과할 수 있어, 실측 가능한 합리적
  기본값으로 시작하고 env var로 조정 가능하게 열어뒀다.

## 확인 방법 (실제로 수행함)

```
./gradlew test   # 신규 회귀 테스트 포함 4개 전체 통과
```

- 실제 브로커+Redis로 재기동 → 로그에서 `fds-v2-redis-feature-sink` 컨슈머 스레드 4개가
  `account-feature-updates`의 32개 파티션을 8개씩 나눠 갖는 것 확인 (concurrency 적용 확인).
- 정상 거래 발행 → Redis에 여전히 정확한 값으로 쓰이는 것 확인 (에러 핸들러/concurrency 추가가
  정상 경로를 깨지 않았음을 재확인).

## 다음에 이어서 할 일

- PR #5 push 및 머지.
- CP3 패널 확장(Redis Exporter), CP3용 k6 시나리오 — 다음 세션 후보.
- CP4(PyTorch 모델 서빙) 착수 여부 논의.
