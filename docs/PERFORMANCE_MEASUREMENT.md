# FDS v2 성능 측정 체계

이 프로젝트는 기존 FDS 프로젝트와 별개의 신규 프로젝트다. 다만 기능별 성능 비교(개선 전/후)를 위해
Prometheus + Grafana로 지표를 수집하고, k6로 부하를 발생시켜 측정한다.

각 체크포인트(CP)는 `ARCHITECTURE.md`의 5단계 파이프라인 경계와 1:1로 대응한다.

---

## CP1 — 거래 이벤트 수집 (Kafka 프로듀서/브로커)

**측정 목적**: 파티셔닝 전략이 처리량과 순서 보장에 미치는 영향 확인

| 지표 | 수집 방식 | 확인 포인트 |
|---|---|---|
| 프로듀서 발행 TPS | Prometheus (Kafka producer metrics exporter) | 목표 TPS 대비 실제 처리량 |
| 파티션별 메시지 분포 | Kafka JMX exporter → Prometheus | 특정 파티션 쏠림(핫 파티션) 여부 |
| 프로듀서 발행 latency (p95/p99) | Prometheus | 파티션 키 해시 연산 오버헤드 확인 |
| 컨슈머 랙(lag) | Kafka Exporter | 수집 속도가 처리 속도를 못 따라가는지 |

**k6 시나리오**: 특정 계좌 ID(고빈도 가정 계좌)에 트래픽을 집중시켜 핫 파티션 발생 여부를 재현하는 테스트, 이후 salting 적용 전/후 파티션별 분포를 비교.

---

## CP2 — 실시간 시퀀스 집계 (Kafka Streams)

**측정 목적**: State Store 조회/갱신 성능과 장애 복구 시간 확인

| 지표 | 수집 방식 | 확인 포인트 |
|---|---|---|
| 레코드 처리 latency | Kafka Streams metrics (process-latency-avg/max) → Prometheus | 슬라이딩 윈도우 계산 오버헤드 |
| State Store(RocksDB) 조회/쓰기 latency | RocksDB metrics exporter | 로컬 디스크 I/O 병목 여부 |
| Changelog topic 쓰기 lag | Kafka Streams metrics | 장애 복구 시 replay 소요 시간과 연관 |
| 리밸런싱 발생 횟수/소요시간 | Kafka Streams metrics | 인스턴스 스케일 조정 시 순간 지연 |

**k6 시나리오**: 특정 계좌에 짧은 시간 내 다건 거래를 연속 발생시켜, 슬라이딩 윈도우 통계(최근 5분 횟수 등)가 실시간으로 정확히 갱신되는지 및 그 소요 시간을 측정.

---

## CP3 — 온라인 피처 스토어 (Redis)

**측정 목적**: 조회 응답 시간과 TTL 정책의 메모리 관리 효과 확인

| 지표 | 수집 방식 | 확인 포인트 |
|---|---|---|
| GET/SET latency (p50/p95/p99) | Redis Exporter → Prometheus | 1ms 이내 목표 달성 여부 |
| 캐시 히트율 | Redis Exporter (keyspace hits/misses) | 피처 스토어가 실제로 재계산을 줄이는지 |
| TTL 만료로 인한 자동 삭제 건수 | Redis Exporter (expired_keys) | 비활성 계좌 정리가 의도대로 동작하는지 |
| 메모리 사용량 추이 | Redis INFO → Prometheus | 활성 계좌 수 증가에 따른 메모리 증가율 |

**k6 시나리오**: 동시 다발 계좌 조회 요청을 발생시켜 Redis 응답 시간이 목표 SLA(예: p99 5ms 이내) 안에 드는지 확인.

---

## CP4 — PyTorch 시퀀스 모델 서빙

**측정 목적**: 추론 latency와 장애 시 폴백 전환 시간 확인

| 지표 | 수집 방식 | 확인 포인트 |
|---|---|---|
| 추론 latency (p50/p95/p99) | TorchServe metrics → Prometheus | Circuit Breaker 타임아웃 값 산정 근거 |
| 배치 크기별 처리량 | TorchServe metrics | 배치 처리 vs 단건 처리 트레이드오프 |
| 타임아웃/서킷브레이커 오픈 발생률 | Resilience4j metrics → Prometheus | 폴백 전환이 얼마나 자주 발생하는지 |
| 폴백 발생 시 규칙 기반 스코어 応답 latency | 자체 계측 | 폴백 경로가 실제로 빠른지 검증 |

**k6 시나리오 + 장애 주입(Fault Injection)**: 정상 부하 테스트 후, 모델 서버에 인위적 지연/오류를 주입하여 Circuit Breaker가 설계한 타임아웃 대로 열리고 폴백으로 전환되는지 확인.

---

## CP5 — 판정 및 대응 (규칙 엔진 + 앙상블)

**측정 목적**: End-to-end 응답 시간과 액션 분포 확인

| 지표 | 수집 방식 | 확인 포인트 |
|---|---|---|
| End-to-end latency (수집→판정 전체) | Spring Boot Actuator + Micrometer → Prometheus | 전체 SLA(예: 500ms 이내) 준수 여부 |
| 액션별 분포 (허용/추가인증/차단) | 자체 계측 → Prometheus Counter | 임계값이 너무 엄격/느슨한지 판단 근거 |
| 앙상블 결합 연산 latency | 자체 계측 | 병목이 아닌지 확인 (보통 매우 짧아야 정상) |

**k6 시나리오**: 실제 트래픽 패턴과 유사한 혼합 시나리오(정상 90% + 이상 패턴 10%)로 전체 파이프라인 end-to-end latency와 액션 분포를 함께 측정.

---

## Grafana 대시보드 구성 원칙

- 패널을 CP1~CP5 순서대로 배치해서, 대시보드를 위에서 아래로 보면 파이프라인 흐름과 일치하도록 구성.
- 각 CP마다 latency(p50/p95/p99)와 처리량(TPS)을 한 화면에 같이 표시 → 병목 구간을 한눈에 비교 가능하게.
- k6 실행 시점을 Grafana annotation으로 표시해서, 부하 테스트 전/후 지표 변화를 시각적으로 대조.

## 개선 전/후 비교 방법

기존 FDS(단건 판단)와 새 FDS(시퀀스 기반)의 성능을 비교할 때는 아래 두 가지를 반드시 함께 제시한다.

1. **정확도/탐지력 관점**: 시퀀스 기반이 단건 대비 어떤 패턴을 추가로 잡아내는지 (정량 지표가 있다면 오탐률/탐지율)
2. **성능 비용 관점**: 시퀀스 집계·모델 서빙 단계가 추가되면서 늘어난 latency가 SLA 안에 드는지

두 관점을 함께 봐야 "정확도는 올랐지만 응답 시간이 SLA를 넘겼다" 같은 실무적 트레이드오프를 놓치지 않는다.

---

## 테스트 결과 저장 방법 — Grafana 스냅샷 자동 저장

k6 부하테스트를 돌릴 때마다 그 시점의 Grafana 대시보드를 이미지로 남겨서, 나중에 개선 전/후 비교 근거로 쓴다.

### 사전 준비 — Grafana Image Renderer 플러그인

Grafana 공식 플러그인. Docker Compose에 렌더러 컨테이너를 하나 추가하면 특정 패널/대시보드를 PNG로 받을 수 있는 렌더링 URL이 활성화된다.

### 스냅샷 저장 명령

```bash
curl -H "Authorization: Bearer <API_KEY>" \
  "http://localhost:3000/render/d/<dashboard-uid>/fds-v2-performance?orgId=1&from=now-30m&to=now&width=1600&height=900" \
  -o docs/performance-results/2026-09-03_cp1-partitioning.png
```

### k6 테스트 스크립트에 후처리로 연결 (자동화)

```bash
k6 run cp1-hot-partition-test.js
sleep 5  # 지표가 Prometheus에 반영될 시간 확보
curl -H "Authorization: Bearer <API_KEY>" \
  "http://localhost:3000/render/d/<dashboard-uid>/fds-v2-performance?orgId=1&from=now-30m&to=now&width=1600&height=900" \
  -o docs/performance-results/$(date +%Y-%m-%d_%H%M)_cp1.png
```

### 저장 위치

```
docs/
└── performance-results/
    ├── 2026-09-03_cp1-hot-partition-before.png
    ├── 2026-09-03_cp1-hot-partition-after-salting.png
    └── README.md   # 각 스크린샷이 어떤 테스트 조건이었는지 기록
```

`README.md`에는 스크린샷 파일명과 함께 "어떤 CP, 어떤 조건(before/after), 어떤 k6 시나리오로 찍은 것인지"를 한 줄씩 남겨서, 나중에 포트폴리오 발표 자료에 바로 가져다 쓸 수 있게 한다.
