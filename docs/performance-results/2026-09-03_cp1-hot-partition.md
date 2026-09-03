# CP1 — 핫 파티션 재현 테스트 결과

- 날짜: 2026-09-03
- 스크립트: `k6/cp1-hot-partition-test.js`
- 환경: 로컬 단일 브로커 Kafka(docker-compose, KRaft), 앱은 `bootRun`, 토픽 파티션 32개
- 부하 프로파일: ramping-vus 0→20(10s)→20 유지(30s)→0(10s), 두 시나리오 모두 동일

Grafana Image Renderer 스택이 아직 없어서 PNG 스냅샷 대신, k6 summary 출력과 앱의
`TransactionEventConsumer` 로그(accountId, partition)를 직접 집계한 텍스트 결과로 기록한다.
스택이 구축되면 이 문서는 스크린샷 기반 기록으로 교체 예정.

## SCENARIO=hot (고빈도 계좌 1개에 집중)

- 요청: `k6 run -e SCENARIO=hot -e BASE_URL=http://localhost:18080 k6/cp1-hot-partition-test.js`
- 총 요청: 15,535건 (실패 0건, 202 100%)
- TPS: 310.4 req/s
- latency: avg=1.01ms, p90=1.56ms, p95=1.81ms
- **파티션 분포: `acc-hot-001`의 15,535건 전부 partition=13 — 100% 단일 파티션 집중 (핫 파티션 재현 확인)**

## SCENARIO=baseline (계좌 200개로 분산)

- 요청: `k6 run -e SCENARIO=baseline -e BASE_URL=http://localhost:18080 k6/cp1-hot-partition-test.js`
- 총 요청: 15,544건 (실패 0건, 202 100%)
- TPS: 310.9 req/s
- latency: avg=0.98ms, p90=1.56ms, p95=1.76ms
- **파티션 분포: 32개 파티션 전체에 분산 (최다 partition=2 928건 ~ 최소 partition=16 181건, 약 5.1배 차이 — accountId 200개를 32개 파티션에 해시한 결과로는 정상적인 편차 범위)**

## 해석

- 같은 조건(TPS, VU 수)에서 hot과 baseline의 HTTP latency 자체는 거의 차이 없음 — 이 앱은 프로듀서가 파티션을 정하고 비동기로 발행만 하기 때문에, **REST 응답 시간에는 핫 파티션의 영향이 바로 드러나지 않는다.**
- 핫 파티션의 실제 문제는 **컨슈머 측 단일 파티션 처리량 병목** (같은 계좌의 이벤트를 처리하는 Kafka Streams 태스크 하나에 부하가 집중됨)과 **파티션 리더 브로커의 쏠림**인데, 이번 세션은 이걸 정량적으로 측정할 컨슈머 랙/리밸런싱 지표(CP2, Kafka Streams 토폴로지)가 아직 없어서 확인하지 못했다.
- 따라서 이번 결과는 "파티셔닝 전략이 설계대로 동작한다(같은 계좌→같은 파티션 100% 보장)"는 것과 "핫 계좌가 실제로 파티션 1개에 쏠린다"는 것까지만 확인한 것이고, **그로 인한 성능 저하 자체는 CP2(Kafka Streams 슬라이딩 윈도우 집계) 구현 이후 컨슈머 랙/처리 latency로 다시 측정해야** 의미가 있다.

## 다음에 필요한 것

- Prometheus + Kafka JMX exporter + Grafana 스택 구축 (docs/PERFORMANCE_MEASUREMENT.md CP1 지표 표의 "파티션별 메시지 분포", "컨슈머 랙" 항목은 이 스택 없이는 측정 불가)
- salting 적용 후 동일 hot 시나리오 재실행 → 파티션 분포 before/after 비교
