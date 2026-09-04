# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, backend/kafka-streams-observability, session-02

## 이번 세션에서 다룬 질문/요청

- "순서대로 계속 해줘" — PR #4(CP2 패널 확장)를 `/code-review`로 리뷰 후 반영.

## 변경/결정된 내용

- `/code-review`로 PR #4를 medium 강도로 리뷰 — 5개 항목 발견, 전부 반영:
  1. **`kafkaStreamsMicrometerConfigurer` 빈 삭제**: Spring Boot 4.1.1이
     `KafkaMetricsAutoConfiguration.KafkaStreamsMetricsConfiguration`으로 이미 똑같은
     `StreamsBuilderFactoryBeanConfigurer`(`KafkaStreamsMicrometerListener` 등록)를 자동으로
     제공한다는 걸 jar 바이트코드로 직접 확인(`@ConditionalOnClass({KafkaStreamsMetrics.class,
     StreamsBuilderFactoryBean.class})`, 둘 다 클래스패스에 있어 항상 활성화됨). 직접 만든 빈은
     완전히 중복이었음 — 삭제.
  2. **화이트리스트 필터를 `spring.id` 태그로 스코프 한정**: 기존엔 이름 접두사(`kafka.stream.`
     등)만 보고 전역으로 걸었는데, 이러면 나중에 이 앱에 다른 순수 컨슈머/어드민 클라이언트가
     생겨도 이름이 겹친다는 이유로 지표가 영원히 숨겨질 위험이 있었다. `Meter.Id.getTag("spring.id")
     == "defaultKafkaStreamsBuilder"`로 한정해서, Kafka Streams가 만든 클라이언트의 지표만
     걸러지게 함.
  3. **`kafka.producer.*`도 필터 대상에 포함**: `metrics.recording.level=DEBUG`가 Kafka Streams
     내부 프로듀서(changelog/출력 토픽 쓰기용)의 기록 레벨도 같이 올린다는 걸 지적받음 — 컨슈머/
     어드민과 똑같이 "이름만 보고 오분류된 음수 카운터"가 나올 수 있는 경로라 미리 포함시킴.
  4. **`metrics.recording.level`을 env var로 오버라이드 가능하게**: `FDS_KAFKA_STREAMS_METRICS_LEVEL`
     추가 — 운영에서 지표 계산 오버헤드/크래시 위험 표면을 낮추고 싶을 때 코드 수정 없이 되돌릴 수
     있게 함.
  5. **화이트리스트 로직 단위 테스트 추가**: `SequenceAggregationTopologyConfigTest` — 허용/차단
     경계 케이스 5개(화이트리스트 안/밖, producer 포함, spring.id 다른 클라이언트는 영향 없음,
     kafka 무관 지표는 그대로 통과).

## 설계 의도 및 트레이드오프

- **`spring.id` 태그 존재를 전제로 한 스코핑**: Spring Kafka의 `KafkaMetricsSupport`가 내부적으로
  모든 지표에 `spring.id` 태그를 붙인다는 걸 jar 바이트코드로 확인하고 나서 이 방식을 택했다 —
  가정이 아니라 실측 근거가 있는 설계.
- **중복 빈을 몰랐던 이유**: 처음 구현할 때 "Boot가 뭘 자동으로 해주는지"를 검색 대신 API 시그니처
  확인(서브에이전트로 `FixedKeyProcessor` 등은 검증했음)에만 집중했고, `KafkaMetricsAutoConfiguration`
  존재 자체를 몰랐다 — 코드 리뷰가 아니었으면 계속 중복 등록 상태로 남았을 것. "새 빈을 만들기 전에
  Boot 자동 설정이 이미 하고 있는지 먼저 확인한다"는 걸 다음부터의 체크리스트로 남긴다.

## 막혔던 문제와 해결 방법

- **재검증 중 Docker Desktop이 죽어 있었음**: 15시간 전에 Exited 상태(아마 컴퓨터 절전/재부팅)였던
  걸 발견 — Docker Desktop을 재시작하고 컨테이너를 다시 `docker start`, kafka-exporter는 kafka
  healthcheck 순서 문제로 한 번 더 재시작 필요(이전에도 겪었던 것과 같은 종류의 레이스, `docker
  start`는 `depends_on: condition: service_healthy`를 존중하지 않아서 발생).
- **포트 18080 충돌**: 이전 세션에서 남은 java 프로세스가 안 죽어 있었음 — `taskkill`로 정리 후 재시도.

## 확인 방법 (실제로 수행함)

```
./gradlew test                                # 5개 신규 테스트 포함 전체 통과
k6 run k6/cp2-sequence-aggregation-test.js    # 계좌 1개, 10 VU, 60초
```

- 수정 후 `/actuator/prometheus`가 계속 200이고, `kafka_stream_thread_process_latency_avg`/
  `kafka_stream_state_put_latency_avg` 등이 실제 값으로 채워지는 것 확인 (리스너 중복 제거가 지표
  자체를 깨뜨리지 않았음을 확인).
- `kafka_producer_*` 지표 중 `spring_id="defaultKafkaStreamsBuilder"` 태그가 붙은 게 하나도 없는
  것 확인 — 새로 추가한 producer 필터가 실제로 적용되고 있음.

## 다음에 이어서 할 일

- PR #4 push 및 merge, CP3(`backend/redis-feature-store`) 착수.
