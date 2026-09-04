import http from 'k6/http';
import { check, sleep } from 'k6';

// CP2 — 실시간 시퀀스 집계 부하 시나리오 (docs/PERFORMANCE_MEASUREMENT.md CP2 참고)
//
// 특정 계좌 하나에 짧은 시간 안에 다건의 거래를 연속 발생시켜서:
//   1) 슬라이딩 윈도우 통계(recentWindowCount 등)가 실시간으로 정확히 갱신되는지
//   2) Kafka Streams 자체 지표(레코드 처리 latency, RocksDB put/get latency 등,
//      docs/MONITORING_SETUP.md/Grafana "FDS v2 - CP2 Kafka Streams" 대시보드)가
//      의미 있는(NaN이 아닌) 값으로 채워지는지
// 를 함께 확인한다. CP1의 hot-partition 시나리오와 목적이 다르다 — CP1은 "파티션이 쏠리는가"를
// 보는 거고, 이건 "그 쏠린 파티션 하나를 Kafka Streams가 얼마나 빠르게/안정적으로 처리하는가"를 본다.
//
// 실행 예:
//   k6 run k6/cp2-sequence-aggregation-test.js

const BASE_URL = __ENV.BASE_URL || 'http://localhost:18080';
const ACCOUNT_ID = __ENV.ACCOUNT_ID || 'acc-cp2-load';

export const options = {
  scenarios: {
    default: {
      executor: 'constant-vus',
      vus: 10,
      duration: '60s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const payload = JSON.stringify({
    transactionId: `tx-${ACCOUNT_ID}-${__VU}-${__ITER}-${Date.now()}`,
    accountId: ACCOUNT_ID,
    amount: Number((Math.random() * 100000).toFixed(2)),
    merchantCategory: 'RETAIL',
    country: Math.random() < 0.1 ? 'US' : 'KR', // 가끔 국가를 바꿔서 countryChanged=true 케이스도 섞음
    occurredAt: new Date().toISOString(),
  });

  const res = http.post(`${BASE_URL}/api/transactions`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    '202 accepted': (r) => r.status === 202,
  });

  sleep(0.02);
}
