# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-03, backend/observability-jvm-panels, session-01

## 이번 세션에서 다룬 질문/요청

- "jvm, cpu, db 같은 지표들도 다 보면 좋지 않을까?" — CP2 PR(#2) 이후 다음 작업을 고르던 중, 일반적인
  앱 헬스 지표(JVM/CPU/RocksDB) 관측 필요성에 대한 질문.
- 범위를 "JVM/CPU만 먼저"로 좁히고, 별도 브랜치(CP2 PR과 섞이지 않게)로 진행하기로 확인.

## 변경/결정된 내용

- **`monitoring/grafana/provisioning/dashboards/json/app-health.json`** 신규 — "FDS v2 - App Health
  (JVM/CPU)" 대시보드, 4개 패널:
  - JVM 힙 메모리 사용량 (`jvm_memory_used_bytes`/`jvm_memory_max_bytes`)
  - CPU 사용률 — 프로세스 vs 시스템 (`process_cpu_usage`/`system_cpu_usage`)
  - GC 정지 시간 비율 (`rate(jvm_gc_pause_seconds_sum[1m])`)
  - JVM 스레드 수 (`jvm_threads_live_threads`/`jvm_threads_daemon_threads`)
- 새 계측 코드는 없음 — Spring Boot Actuator/Micrometer가 CP1 때부터 이미 이 지표들을
  `/actuator/prometheus`로 노출하고 있었고, Grafana 패널만 추가하면 되는 작업이었음.

## 설계 의도 및 트레이드오프

- **CP1 대시보드에 패널을 추가하지 않고 새 대시보드로 분리**: JVM/CPU는 특정 CP(체크포인트)에 속한
  지표가 아니라 앱 전체에 걸친 범용 헬스 지표다. `docs/PERFORMANCE_MEASUREMENT.md`의 "Grafana 대시보드
  구성 원칙"이 CP1~CP5 순서 배치를 전제로 하므로, 여기에 맞지 않는 범용 지표를 CP1 대시보드에 억지로
  끼워 넣기보다 별도 대시보드("FDS v2 - App Health")로 분리하는 게 구조적으로 맞다고 판단.
- **RocksDB/브로커 JVM은 이번 범위에서 제외**: 사용자가 던진 질문("jvm, cpu, db")에서 "db"에 해당하는
  RocksDB(Kafka Streams State Store)는 `metrics.recording.level=DEBUG` 설정 변경 + 바인딩 코드가
  필요한 별도 작업이라, CP2 패널 확장(다음 세션 후보)으로 남겨두고 이번엔 "이미 공짜로 수집되는
  지표"만 다뤘다. 브로커 자체의 JVM/CPU(JMX exporter 필요)도 CP1 모니터링 세션에서 의도적으로
  뺐던 부분이라 이번에도 범위 밖으로 유지.
- **별도 브랜치로 분리한 이유**: CP2 PR(#2)은 Kafka Streams 토폴로지 로직에 집중하는 게 리뷰하기
  쉽고, JVM/CPU 패널은 그 로직과 무관한 관측 설정 변경이라 섞으면 PR 성격이 흐려짐. main에서 새로
  분기해서 독립적으로 진행.

## 막혔던 문제와 해결 방법

- 없음 — 이미 CP1 때 만들어둔 Prometheus/Grafana 프로비저닝 구조를 그대로 재사용해서 대시보드 JSON
  파일 하나만 추가하면 되는 작업이었음. 다만 실행 중인 스택이 `fds-backend-kafka` worktree 경로에
  마운트돼 있어서, 이 브랜치(`fds-observability` worktree)에서 작성한 파일을 직접 라이브 검증할 수
  없었음 — 검증용으로 해당 파일을 그 경로에 임시 복사해서 확인한 뒤, 커밋은 이 브랜치에만 남기고
  임시 복사본은 삭제해서 다른 worktree의 git 상태를 오염시키지 않았음.

## 확인 방법 (실제로 수행함)

- CP1 때 만든 docker-compose 스택(kafka/kafka-exporter/prometheus/grafana)이 이미 떠 있는 상태를
  그대로 재사용.
- 앱을 `SERVER_PORT=18080`(Prometheus 스크레이프 타깃과 일치)으로 기동 후, Prometheus에 4개 쿼리를
  직접 날려서 전부 값이 나오는 것 확인 (`process_cpu_usage`, `sum(jvm_memory_used_bytes{area="heap"})`,
  `jvm_threads_live_threads`, `sum(rate(jvm_gc_pause_seconds_sum[1m]))`).
- Grafana가 30초 프로비저닝 주기로 새 대시보드를 자동으로 인식하는 것 확인 (`/api/dashboards/uid/fds-v2-app-health`).
- 브라우저로 대시보드를 직접 열어서 4개 패널 모두 실데이터로 렌더링되는 것 스크린샷으로 확인.

## 다음에 이어서 할 일

- CP2 패널 확장(Kafka Streams 자체 지표 — process-latency, changelog lag, 리밸런싱 등) — CP2 PR(#2)과
  같이 진행하거나 이어서 별도로.
- RocksDB(State Store) 지표를 보려면 `metrics.recording.level=DEBUG` + Micrometer 바인딩 필요 (다음
  개선 후보).
- 브로커 자체 JVM/CPU를 보려면 JMX exporter 도입 여부를 별도로 결정 (CP1 모니터링 세션에서 이미
  한 번 미룬 결정, 아직 유효).
