# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-03, backend/kafka-streams-observability, session-01

## 이번 세션에서 다룬 질문/요청

- "순서대로 해줘" — PR #2(CP2) 리뷰·머지 → **CP2 패널 확장** → CP3 착수 순서로 진행. 이 로그는
  그중 "CP2 패널 확장" 단계.
- 목표: `docs/PERFORMANCE_MEASUREMENT.md` CP2 표(레코드 처리 latency, State Store latency,
  changelog lag, 리밸런싱)에 대응하는 Kafka Streams 자체 지표를 CP1 Prometheus/Grafana 스택에 연결.

## 변경/결정된 내용

- `SequenceAggregationTopologyConfig`에 `KafkaStreamsMicrometerListener`(Spring Kafka 공식
  확장 포인트, `StreamsBuilderFactoryBeanConfigurer` 경유)를 등록해서 Kafka Streams 자체 지표를
  Micrometer/Prometheus로 노출.
- `application.yml`에 `spring.kafka.streams.properties.metrics.recording.level: DEBUG` 추가 —
  RocksDB State Store 지표는 기본값(INFO)에서 아예 안 잡힘.
- **화이트리스트 MeterFilter**(`kafkaStreamsMetricsAllowlistFilter`) 추가 — 아래 "막혔던 문제" 참고.
- `monitoring/grafana/provisioning/dashboards/json/cp2-kafka-streams.json` 신규 — 5개 패널
  (레코드 처리 latency, State Store put/get latency, 커밋 latency, 리밸런싱 latency/빈도, 복구 중인
  태스크 수).
- `k6/cp2-sequence-aggregation-test.js` 신규 — 계좌 1개에 지속 부하(10 VU, 60초)를 걸어서 위 지표들이
  실제 값으로 채워지는지 확인하는 시나리오.
- `docs/BACKEND.md`에 "CP2 관측 확장" 섹션, `docs/performance-results/`에 대시보드 스크린샷 추가.

## 설계 의도 및 트레이드오프

- **블랙리스트 → 화이트리스트 전환**: `KafkaStreamsMicrometerListener`를 필터 없이 붙였더니
  `/actuator/prometheus`가 "counters cannot have a negative value" 예외로 통째로 500이 나는 걸
  실측 중 발견했다. 원인은 이 리스너가 raw Kafka 지표를 이름만 보고(`-total`로 끝나면 무조건
  누적 카운터) 기계적으로 분류하는데, 이름과 달리 실제로는 감소하는 값(예: restore 진행 중 줄어드는
  "남은 레코드 수")이 섞여 있어서다. 문제 지표를 하나씩 이름으로 차단하는 방식을 먼저 시도했는데,
  k6로 부하를 다시 걸자 또 다른 지표가 똑같이 크래시를 냈다 — 어떤 지표가 언제 음수를 낼지 미리 다
  알 수 없다는 뜻이라, 블랙리스트를 포기하고 "CP2가 실제로 필요로 하는 지표만 명시적으로 허용"하는
  화이트리스트로 뒤집었다. 앞으로 어떤 새 지표가 음수를 내든 애초에 노출이 안 되므로 안전하다.
- **"changelog 쓰기 lag" 직접 지표는 포기**: 그 지표(restore-remaining-records-total)가 바로 이번에
  문제를 일으킨 그 지표라, 화이트리스트에서 빠졌다. 대신 이미 있는 `kafka_consumergroup_lag`(CP1
  kafka-exporter)와 새로 추가한 `active-restoring-tasks`(복구 진행 여부)를 근사치로 쓰기로 했다 —
  완벽하진 않지만 크래시 위험을 감수하면서까지 얻을 가치는 없다고 판단.
- **RocksDB put/get latency 패널의 legend가 32개 task로 지저분함**: task별로 지표가 나뉘는 게
  Kafka Streams의 정상 동작(파티션=task이므로)이라, 필터링하지 않고 그대로 뒀다 — 실제 트래픽이
  몰린 task만 값이 채워지고 나머지는 NaN(빈 라인)이라 패널 설명에 이유를 명시했다.

## 막혔던 문제와 해결 방법

1. **`/actuator/prometheus`가 -1.0 값 때문에 500 (`restore-remaining-records-total`)**: 위 "설계
   의도" 참고. 해당 지표 하나만 `MeterFilter.deny`로 차단해서 1차 해결.
2. **k6로 60초 부하를 다시 걸자 -3.0 값의 다른 지표가 똑같은 예외를 냄**: 1번의 블랙리스트 방식이
   근본적으로 불안정하다는 걸 확인 → 화이트리스트(`MeterFilter.denyUnless`)로 전면 전환. 이후 같은
   k6 시나리오를 두 번 연속 돌려도 500이 재현되지 않는 것까지 확인.
3. **RocksDB 지표가 `/actuator/metrics`에는 등록되어 있는데 `/actuator/prometheus`엔 안 보임**: 값이
   `NaN`이라 Prometheus 익스포터가 조용히 걸러낸 것 — 실제 값이 없어서가 아니라 트래픽이 부족해서
   (32개 task 중 실제 접근이 있었던 task만 값이 채워짐). k6로 지속 부하를 걸어서 해결(값이 채워짐).
4. **리밸런싱 패널에서 latency(ms)와 발생 빈도(회/시간)가 같은 y축 단위로 섞여 나옴**: 쿼리에
   `refId`를 명시하고 `fieldConfig.overrides`로 세 번째 시리즈(발생 빈도)만 별도 unit(`short`)과
   오른쪽 축으로 분리해서 해결.

## 확인 방법 (실제로 수행함)

```
k6 run k6/cp2-sequence-aggregation-test.js   # 계좌 1개, 10 VU, 60초, ~460 req/s
```

- 화이트리스트 적용 전: 위 시나리오 실행 후 `/actuator/prometheus`가 500 (재현 2회, 서로 다른 지표).
- 화이트리스트 적용 후: 같은 시나리오를 연속 두 번 실행해도 `/actuator/prometheus`가 계속 200.
- Prometheus 쿼리로 5개 핵심 지표(process/put/get/commit latency, rebalance rate)가 실제 값으로
  채워지는 것 확인 (예: put latency avg ≈ 19ms, get latency avg ≈ 17ms, process latency avg ≈ 37ms).
- Grafana `FDS v2 - CP2 Kafka Streams` 대시보드 5개 패널 전부 실데이터 렌더링 확인, 스크린샷 저장.

## 다음에 이어서 할 일

- `restore-remaining-records-total`을 커스텀 Gauge로 직접 등록해서 안전하게(음수 허용되는 Gauge로)
  노출하는 방법 검토 — "진짜 changelog lag" 시각화가 필요해지면.
- CP3(`backend/redis-feature-store`) 착수.
