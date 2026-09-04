# 성능 측정 결과 기록

각 스크린샷/결과 파일이 어떤 CP, 어떤 조건(before/after), 어떤 k6 시나리오로 찍은 것인지 한 줄씩 기록한다.
저장 규칙은 `docs/PERFORMANCE_MEASUREMENT.md`의 "테스트 결과 저장 방법" 참고.

| 파일 | CP | 조건 | k6 시나리오 | 비고 |
|---|---|---|---|---|
| [2026-09-03_cp1-hot-partition.md](./2026-09-03_cp1-hot-partition.md) | CP1 | hot vs baseline | `cp1-hot-partition-test.js` | Grafana 스택이 아직 없어 PNG 대신 텍스트 기록. hot=파티션 1개 100% 집중(15,535건), baseline=32개 파티션 전체 분산 확인. |
| [2026-09-03_cp1-grafana-dashboard.jpg](./2026-09-03_cp1-grafana-dashboard.jpg) | CP1 | hot | `cp1-hot-partition-test.js` | Prometheus+kafka-exporter+Grafana 스택 구축 후 첫 대시보드 스크린샷. 파티션별 메시지 분포 패널에서 partition 13(15,536건)만 튀는 것을 시각적으로 확인 — 위 텍스트 기록과 동일한 결과를 대시보드로 재현. 자세한 스택 구성/설계 결정은 `docs/MONITORING_SETUP.md` 참고. |
| [2026-09-03_cp2-kafka-streams-dashboard.jpg](./2026-09-03_cp2-kafka-streams-dashboard.jpg) | CP2 | 지속 부하(10 VU, 60s, 계좌 1개) | `cp2-sequence-aggregation-test.js` | Kafka Streams 자체 지표(레코드 처리 latency, RocksDB put/get latency, 커밋 latency, 리밸런싱 latency/빈도)를 Micrometer/Grafana에 연결. `KafkaStreamsMicrometerListener`가 raw Kafka 지표를 무분별하게 바인딩하면서 이름과 실제 의미가 다른 지표(예: restore-remaining-records-total)가 음수 값을 내 `/actuator/prometheus` 전체가 500 나는 문제를 두 번 겪은 뒤 화이트리스트 방식으로 전환 — 자세한 내용은 세션 로그 참고. |
| [2026-09-04_cp3-redis-dashboard.jpg](./2026-09-04_cp3-redis-dashboard.jpg) | CP3 | 워밍업(계좌 50개) + 조회 부하(20 VU, 30s, 미스율 10%), TTL=1분으로 오버라이드 | `cp3-feature-lookup-test.js` | Redis Exporter로 GET/SET latency(p50/p99), 캐시 히트율, TTL 만료 건수, 메모리 사용량을 Grafana에 연결. 캐시 히트율이 의도한 90%(미스율 10%)와 실측 일치, TTL 만료도 61건 실제 발생 확인. 측정 신호를 만들기 위해 "측정 전용" `GET /api/features/{accountId}` 엔드포인트를 이 브랜치에서 추가(CP3 구현 세션에서 "CP4 몫"이라 미뤘던 것과 상충되는 결정이라 트레이드오프를 세션 로그에 명시). |
