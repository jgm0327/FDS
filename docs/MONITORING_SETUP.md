# CP1 모니터링 스택 (Prometheus + kafka-exporter + Grafana)

`docs/PERFORMANCE_MEASUREMENT.md`의 CP1 표가 요구하는 지표(파티션별 메시지 분포, 컨슈머 랙,
프로듀서 TPS/latency)를 실제로 관측하기 위한 로컬 스택. `backend/kafka-partitioning` 브랜치
범위에서 구축했다.

## 기동 방법

```bash
docker compose up -d
SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 ./gradlew bootRun
```

- Grafana: http://localhost:13000 (익명 접속 Admin 권한 — 아래 "보안 관련 참고" 참조)
- Prometheus: http://localhost:19090
- 대시보드: Dashboards → FDS v2 → **FDS v2 - CP1 Kafka Partitioning** (자동 프로비저닝됨, 별도 로그인/설정 불필요)

앱을 `SERVER_PORT` 오버라이드 없이 기본 포트(8080)로 띄웠다면, `monitoring/prometheus/prometheus.yml`의
`fds-v2-backend` 타깃을 8080으로 바꿔야 Prometheus가 스크레이프할 수 있다.

## 구성 요소

| 컨테이너 | 역할 | 호스트 포트 |
|---|---|---|
| `fds-v2-kafka` | 단일 브로커 Kafka (기존) | 19092 |
| `fds-v2-kafka-exporter` | 토픽/파티션 offset, 컨슈머 그룹 lag를 Prometheus 포맷으로 노출 | 19308 |
| `fds-v2-prometheus` | 메트릭 수집/저장 | 19090 |
| `fds-v2-grafana` | 대시보드 | 13000 |

호스트 포트는 모두 로컬의 다른 프로젝트(`fsd-api-*`, `pingbell-*`)와 충돌을 피하기 위해
표준 포트(9092/9308/9090/3000)에서 1만 단위를 얹어 옮겼다 — session-02와 동일한 규칙.

Spring Boot 앱은 docker-compose 밖(호스트)에서 `./gradlew bootRun`으로 띄우는 구조라(session-02),
Prometheus는 `host.docker.internal`(Docker Desktop 전용 DNS)로 앱에 접근한다.

## 설계 결정 및 트레이드오프

### 1. 브로커 JMX exporter는 붙이지 않음

CP1 표는 "파티션별 메시지 분포"를 Kafka JMX exporter로 수집하라고 되어 있지만, 실제로 붙이지 않았다.

- **프로듀서 TPS/latency**는 이미 앱의 Micrometer Kafka client metrics(`kafka_producer_*`, session-02에서
  `kafka_producer_batch_size_avg` 노출 확인함)와, 이번에 `TransactionEventProducer`에 추가한 자체 Timer
  (`fds_transaction_publish_duration_seconds`)로 충분히 커버된다 — 브로커 쪽 JMX가 필요 없다.
- **파티션별 메시지 분포**와 **컨슈머 랙**은 `kafka_exporter`(danielqsj/kafka-exporter) 하나로 JMX 없이도
  정확히 얻을 수 있다 (`kafka_topic_partition_current_offset`, `kafka_consumergroup_lag` — Kafka 프로토콜로
  직접 조회, JMX 불필요).
- 즉 CP1이 요구하는 4개 지표를 kafka_exporter + 앱 자체 메트릭만으로 전부 채울 수 있어서, 브로커 JVM에
  javaagent를 붙이고 커뮤니티 JMX→Prometheus 매핑 설정을 관리하는 추가 복잡도(컨테이너 1개, 설정 파일
  1개, 볼륨 마운트)를 들일 이유가 없다고 판단했다. 이후 CP2(Kafka Streams State Store/RocksDB 지표 등)에서
  JMX가 실제로 필요해지면 그때 다시 검토한다.

### 2. 프로듀서 발행 latency는 histogram_quantile()로 계산 (client-side publishPercentiles 미사용)

처음에는 Micrometer `Timer.builder(...).publishPercentiles(0.5, 0.95, 0.99)`로 클라이언트 사이드 퍼센타일을
바로 노출하려 했으나, 실제로 `/actuator/prometheus`를 확인해보니 `quantile` 라벨이 붙은 시계열이 전혀
나오지 않는 걸 확인했다 (이 Spring Boot 4 / Micrometer 조합에서의 동작 — 원인은 더 파지 않고 대안으로 전환).
대신 `publishPercentileHistogram()`으로 버킷 히스토그램만 노출하고, Grafana 쿼리에서
`histogram_quantile(0.95, sum by (le) (rate(...[1m])))`로 계산하도록 바꿨다. 이 방식은 여러 인스턴스로
스케일아웃해도 버킷을 합산한 뒤 계산하기 때문에 client-side 방식보다 정확도 면에서도 더 낫다.

### 3. Grafana 익명 접속 + Admin 권한 (로컬 전용)

`GF_AUTH_ANONYMOUS_ENABLED=true` + `GF_AUTH_ANONYMOUS_ORG_ROLE=Admin`으로 로그인 없이 바로 대시보드를 볼 수
있게 했다. 로컬 개발 편의를 위한 설정이며, 운영 환경에 그대로 가져가면 안 된다 (프로덕션 스택을 구축할
일이 생기면 이 값부터 제거).

### 4. Grafana Image Renderer(PNG 자동 스냅샷)는 이번 범위에서 제외

`PERFORMANCE_MEASUREMENT.md`의 "테스트 결과 저장 방법"은 Grafana Image Renderer 플러그인으로 k6 실행 후
PNG를 자동 저장하는 걸 전제로 하는데, 이번 세션은 "관측 가능한 스택을 구축"까지가 목표였고 자동화된 스냅샷
저장은 별도 작업으로 남겨둔다. 지금은 브라우저로 직접 캡처한 스크린샷을 `docs/performance-results/`에
수동으로 남기는 방식으로 대체했다 (`2026-09-03_cp1-grafana-dashboard.jpg` 참고).

## 검증 내역 (실제로 수행함)

1. `docker compose up -d`로 4개 컨테이너(kafka, kafka-exporter, prometheus, grafana) 기동.
   - kafka-exporter가 kafka보다 먼저 접속을 시도하다 죽는 문제 발견 → kafka에 healthcheck 추가하고
     kafka-exporter의 `depends_on`을 `condition: service_healthy`로 바꿔서 해결.
2. Prometheus 타깃 페이지에서 `fds-v2-backend`, `kafka-exporter` 둘 다 `health: up` 확인.
3. `k6 run k6/cp1-hot-partition-test.js`(hot 시나리오, `docs/performance-results/2026-09-03_cp1-hot-partition.md`와
   동일 스크립트)로 트래픽 발생 → Grafana 대시보드에서:
   - 파티션별 메시지 분포 패널: **파티션 13 하나만 15,536건, 나머지 31개 파티션은 0** — session-03에서 로그로
     확인했던 핫 파티션(계좌 `acc-hot-001` → partition 13)과 정확히 일치하는 결과를 이번엔 대시보드로 시각 확인.
   - 프로듀서 발행 TPS: 트래픽 구간에서 최대 ~280 TPS로 스파이크.
   - 프로듀서 발행 latency (p95/p99): 22~26ms 구간에서 관측.
   - 컨슈머 랙: 트래픽 구간에 짧게 튀었다가 곧바로 0으로 수렴 (컨슈머가 처리 속도를 따라감을 확인).
4. 스크린샷을 `docs/performance-results/2026-09-03_cp1-grafana-dashboard.jpg`로 저장.
