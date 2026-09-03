# 성능 측정 결과 기록

각 스크린샷/결과 파일이 어떤 CP, 어떤 조건(before/after), 어떤 k6 시나리오로 찍은 것인지 한 줄씩 기록한다.
저장 규칙은 `docs/PERFORMANCE_MEASUREMENT.md`의 "테스트 결과 저장 방법" 참고.

| 파일 | CP | 조건 | k6 시나리오 | 비고 |
|---|---|---|---|---|
| [2026-09-03_cp1-hot-partition.md](./2026-09-03_cp1-hot-partition.md) | CP1 | hot vs baseline | `cp1-hot-partition-test.js` | Grafana 스택이 아직 없어 PNG 대신 텍스트 기록. hot=파티션 1개 100% 집중(15,535건), baseline=32개 파티션 전체 분산 확인. |
