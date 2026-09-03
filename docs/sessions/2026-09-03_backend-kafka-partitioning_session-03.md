# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-03, backend/kafka-partitioning, session-03

## 이번 세션에서 다룬 질문/요청

- "진행해줘" — session-02에서 남겨둔 "다음에 이어서 할 일" 중 "CP1 k6 시나리오 작성 및 실행, `docs/performance-results/`에 결과 기록"을 이어서 진행

## 변경/결정된 내용

- **`k6/cp1-hot-partition-test.js`**: `PERFORMANCE_MEASUREMENT.md` CP1 시나리오("특정 계좌 ID에 트래픽을 집중시켜 핫 파티션 발생 여부를 재현")를 그대로 구현. `SCENARIO` env var로 `hot`(계좌 1개 고정)/`baseline`(계좌 200개 풀에서 랜덤) 전환 가능.
- **`docs/performance-results/README.md` + `2026-09-03_cp1-hot-partition.md`**: 갱신된 `PERFORMANCE_MEASUREMENT.md`의 "테스트 결과 저장 방법" 규칙에 맞춰 디렉토리와 인덱스를 처음 생성. 아직 Grafana Image Renderer 스택이 없어서 PNG 대신 k6 summary + 앱 로그 기반 텍스트 기록으로 시작.
- **실제 k6 테스트 실행 결과** (로컬 docker-compose Kafka + `bootRun`):
  - hot: 15,535건 전부 partition=13 — 100% 단일 파티션 집중
  - baseline(계좌 200개): 32개 파티션 전체에 분산(최다 928건~최소 181건)
  - 두 시나리오 모두 HTTP latency(p95 ~1.8ms)는 거의 동일 — REST 응답 시간만으로는 핫 파티션 영향이 드러나지 않음을 확인

## 설계 의도 및 트레이드오프

- **salting 전/후 비교 대신 hot/baseline 비교로 범위 축소**: `PERFORMANCE_MEASUREMENT.md`가 원래 요구하는 건 "salting 적용 전/후 비교"지만, salting 자체가 아직 미구현(ARCHITECTURE.md TODO)이라 지금은 "핫 파티션이 실제로 재현되는가"만 증명하는 hot vs baseline 비교로 범위를 좁혔다. salting 구현 시 이 스크립트의 `SCENARIO=hot` 결과를 baseline(before)으로 재사용하면 됨.
- **Grafana/Prometheus/JMX exporter 스택은 이번 세션에 만들지 않음**: `PERFORMANCE_MEASUREMENT.md`가 요구하는 "파티션별 메시지 분포", "컨슈머 랙" 같은 지표는 JMX exporter 없이는 못 얻는다. 이 스택 구축은 Docker Compose에 3~4개 컨테이너를 더 추가하고 Grafana 대시보드/알림 설정까지 필요한 별도 규모의 작업이라, 지금 kafka-partitioning PR 범위에 슬쩍 끼워 넣기보다는 다음에 별도로 결정해서 진행하는 게 맞다고 판단. 대신 지금 확보 가능한 신호(k6 summary, 컨슈머 로그의 accountId→partition 매핑)만으로 "파티셔닝이 설계대로 동작하는가"는 충분히 검증했다.
- **베이스라인 계좌 풀 크기 200**: 32개 파티션에 대해 계좌 수가 너무 적으면 우연히도 분산이 안 돼 보일 수 있고, 너무 많으면 계좌당 재사용(같은 계좌 반복 히트)이 줄어들어 "파티셔닝 자체"보다 "해시 분포"만 보게 됨. 200개면 파티션당 평균 6.25개 계좌가 매핑되어 재사용도 있고 분산도 관찰 가능한 적당한 크기라고 판단.

## 막혔던 문제와 해결 방법

- 없음 — session-02에서 이미 포트 충돌/Jackson 의존성 문제를 해결해둔 덕분에 이번 세션은 앱/브로커 기동, k6 실행, 로그 집계까지 막힘 없이 진행됨.

## 다음에 이어서 할 일

- Prometheus + Kafka JMX exporter + Grafana 스택 구축 여부/범위를 별도로 결정 (CP1의 "파티션별 메시지 분포", "컨슈머 랙" 지표는 이 스택 없이는 측정 불가)
- salting 구현 후 동일 k6 스크립트로 before(이번 결과)/after 비교
- PR 생성/업데이트 후 `backend/kafka-streams-topology`(CP2) 착수 여부 논의
