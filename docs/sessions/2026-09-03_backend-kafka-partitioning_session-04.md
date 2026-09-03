# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-03, backend/kafka-partitioning, session-04

## 이번 세션에서 다룬 질문/요청

- "session log 보고 이어서 진행해줘" — session-01~03 로그를 읽고 이어서 작업.
- session-03이 남긴 세 갈래 다음 작업(salting before/after 비교, Prometheus+JMX exporter+Grafana 스택,
  CP2 착수) 중 무엇부터 할지 사용자에게 확인 → **"Prometheus+JMX exporter+Grafana 스택 구축"** 선택.

## 변경/결정된 내용

- **`docker-compose.yml`**: `kafka-exporter`(danielqsj/kafka-exporter), `prometheus`, `grafana` 3개 서비스 추가.
  - kafka에 `INTERNAL` 리스너(9094)를 추가해서 kafka-exporter가 컴포즈 내부망으로 접속하도록 함.
  - kafka에 healthcheck(`kafka-broker-api-versions.sh`) 추가하고 kafka-exporter의 `depends_on`을
    `condition: service_healthy`로 걸어서, 브로커가 뜨기 전에 exporter가 먼저 접속을 시도하다 죽는
    레이스 컨디션을 없앰.
  - 호스트 포트: kafka-exporter 19308, prometheus 19090, grafana 13000 — 전부 로컬의 다른 프로젝트
    (`fsd-api-*`)와 충돌 피하려고 표준 포트에서 옮김 (session-02와 동일 규칙).
- **`monitoring/prometheus/prometheus.yml`**: `fds-v2-backend`(host.docker.internal:18080), `kafka-exporter`
  두 잡 스크레이프 설정.
- **`monitoring/grafana/provisioning/`**: datasource(Prometheus) 자동 등록 + 대시보드 자동 로드 설정.
- **`monitoring/grafana/provisioning/dashboards/json/cp1-kafka-partitioning.json`**: CP1 4개 패널 대시보드
  (프로듀서 TPS, 발행 latency p95/p99, 파티션별 메시지 분포, 컨슈머 랙).
- **`TransactionEventProducer.java`**: Micrometer `Timer`로 발행 latency 히스토그램(`fds.transaction.publish.duration`)
  추가 — CP1 "프로듀서 발행 latency (p95/p99)" 지표용.
- **`docs/MONITORING_SETUP.md`**: 스택 기동 방법, 구성 요소, 설계 결정/트레이드오프, 검증 내역을 정리한 신규 문서.
- **`docs/performance-results/`**: 실제 대시보드 스크린샷(`2026-09-03_cp1-grafana-dashboard.jpg`) 추가 + README 인덱스 갱신.

## 설계 의도 및 트레이드오프

- **브로커 JMX exporter는 붙이지 않음**: CP1 표는 JMX exporter를 요구하지만, 프로듀서 TPS/latency는 이미
  앱의 Micrometer Kafka client metrics + 자체 Timer로 확보되고, 파티션별 분포/컨슈머 랙은 kafka_exporter
  하나로 JMX 없이 정확히 얻을 수 있다. 4개 지표를 다 채우는 데 JMX가 굳이 필요 없어서, 브로커 JVM에
  javaagent를 붙이고 커뮤니티 설정 파일을 관리하는 복잡도를 피했다. 자세한 근거는 `docs/MONITORING_SETUP.md`
  1번 항목 참고.
- **client-side publishPercentiles 대신 histogram_quantile()**: 처음엔 Micrometer `publishPercentiles()`로
  바로 p95/p99 시계열을 뽑으려 했으나 실제로 노출되지 않는 걸 확인(아래 "막혔던 문제" 참고). 버킷 히스토그램만
  노출하고 Grafana 쿼리에서 `histogram_quantile()`로 계산하는 방식으로 전환 — 다중 인스턴스로 확장해도
  정확히 합산되는 표준적인 방법이라 더 낫다고 판단.
- **Grafana 익명 Admin 접속**: 로컬 개발 편의를 위한 설정이며 운영에는 가져가면 안 된다고 문서에 명시.
- **Grafana Image Renderer(PNG 자동화)는 이번 범위에서 제외**: `PERFORMANCE_MEASUREMENT.md`가 요구하는
  k6 실행 후 자동 PNG 스냅샷 저장은 별도 컨테이너/설정이 더 필요한 작업이라 이번엔 하지 않고, 브라우저로
  직접 캡처한 스크린샷을 수동으로 남기는 것으로 대체했다.

## 막혔던 문제와 해결 방법

1. **Grafana 컨테이너가 뜨지 않음 (`mounting ... read-only file system`)**: 대시보드 JSON을
   `/etc/grafana/provisioning/dashboards/json`에 별도 볼륨으로 마운트하려 했는데, 상위 경로
   (`/etc/grafana/provisioning`)를 이미 읽기 전용 바인드 마운트로 걸어놔서 그 안에 새 마운트포인트를
   만들 수 없었음 → 대시보드 JSON 파일 자체를 `monitoring/grafana/provisioning/dashboards/json/` 아래로
   옮기고, provisioning 트리 전체를 볼륨 하나로만 마운트하도록 바꿔서 해결.
2. **kafka-exporter가 기동 직후 죽음 (`connect: connection refused`)**: `docker compose up -d` 직후
   kafka-exporter가 kafka보다 먼저 접속을 시도해서 발생. kafka에 healthcheck를 추가하고 kafka-exporter의
   `depends_on`을 `condition: service_healthy`로 바꿔서 브로커가 실제로 준비된 뒤에만 기동하도록 해결.
3. **Micrometer `Timer.builder(...).publishPercentiles(0.5, 0.95, 0.99)`를 붙였는데 `/actuator/prometheus`에
   `quantile` 라벨 붙은 시계열이 전혀 안 보임**: 원인을 깊게 파지는 않고, `publishPercentileHistogram()`만
   남기고 Grafana 쪽에서 `histogram_quantile()`로 p95/p99을 계산하는 방식으로 우회 — 실제 쿼리 결과
   (`histogram_quantile(0.95, ...)` → `0.0217`)로 정상 동작 확인함.

## 확인 방법 (실제로 수행함)

```
docker compose up -d
SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 ./gradlew bootRun
k6 run k6/cp1-hot-partition-test.js
```

- Prometheus 타깃 페이지(`localhost:19090/targets`)에서 `fds-v2-backend`, `kafka-exporter` 둘 다
  `health: up` 확인.
- Grafana 대시보드(`localhost:13000/d/fds-v2-cp1`)에서 4개 패널 모두 실데이터로 렌더링 확인:
  - 파티션별 메시지 분포: **partition 13만 15,536건, 나머지 31개는 0** — session-03에서 로그로 확인했던
    핫 파티션(계좌 `acc-hot-001` → partition 13)과 정확히 일치하는 걸 이번엔 대시보드로 시각화해서 재확인.
  - 프로듀서 TPS: 트래픽 구간에서 최대 ~280 TPS.
  - 발행 latency p95/p99: 22~26ms.
  - 컨슈머 랙: 트래픽 구간에 짧게 튀었다가 바로 0으로 수렴 (컨슈머가 처리 속도를 따라감).
- 대시보드 스크린샷을 `docs/performance-results/2026-09-03_cp1-grafana-dashboard.jpg`로 저장.
- 검증 후 임시로 띄웠던 `bootRun` 프로세스는 종료. docker-compose 스택(kafka/kafka-exporter/prometheus/grafana)은
  사용자가 바로 살펴볼 수 있도록 켜둔 채로 세션을 마침.

## 다음에 이어서 할 일

- salting 구현 후 동일 k6 스크립트로 before(이번 hot 결과)/after 비교 — 이번에 만든 Grafana 대시보드로
  before/after 스크린샷을 나란히 남길 수 있음.
- Grafana Image Renderer 플러그인 추가해서 k6 실행 후 PNG 스냅샷을 자동으로 남기는 자동화 (선택 사항).
- PR 업데이트 후 `backend/kafka-streams-topology`(CP2) 착수 여부 논의.
- CP2에서 State Store(RocksDB) 지표가 필요해지면, 그때 브로커/Streams 쪽 JMX exporter 도입 여부 재검토.
