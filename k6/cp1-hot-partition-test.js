import http from 'k6/http';
import { check, sleep } from 'k6';

// CP1 — 핫 파티션 재현 시나리오 (docs/PERFORMANCE_MEASUREMENT.md CP1 참고)
//
// SCENARIO=hot      (기본값) 고빈도 계좌 1개에 트래픽을 집중시켜 핫 파티션을 재현한다.
// SCENARIO=baseline 계좌 ID를 넓게 분산시켜 정상적인 파티션 분포와 비교한다.
//
// salting 적용 전/후 비교는 salting이 구현된 뒤(docs/ARCHITECTURE.md TODO) 진행할 예정이라,
// 지금은 "핫 파티션이 실제로 재현되는가"(hot) vs "정상 분산 트래픽"(baseline) 비교까지만 다룬다.
//
// 실행 예:
//   k6 run k6/cp1-hot-partition-test.js
//   k6 run -e SCENARIO=baseline k6/cp1-hot-partition-test.js
//
// 파티션별 분포는 아직 JMX exporter/Prometheus가 붙어있지 않아서, 테스트 실행 중 앱의
// TransactionEventConsumer 로그(accountId, partition)를 직접 집계해서 확인한다
// (docs/sessions/2026-09-03_backend-kafka-partitioning_session-03.md 참고).

const BASE_URL = __ENV.BASE_URL || 'http://localhost:18080';
const SCENARIO = __ENV.SCENARIO || 'hot';
const HOT_ACCOUNT_ID = __ENV.HOT_ACCOUNT_ID || 'acc-hot-001';
const BASELINE_ACCOUNT_POOL_SIZE = Number(__ENV.BASELINE_ACCOUNT_POOL_SIZE || 200);

export const options = {
  scenarios: {
    default: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 20 },
        { duration: '30s', target: 20 },
        { duration: '10s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

function pickAccountId() {
  if (SCENARIO === 'hot') {
    return HOT_ACCOUNT_ID;
  }
  const idx = Math.floor(Math.random() * BASELINE_ACCOUNT_POOL_SIZE);
  return `acc-baseline-${idx}`;
}

export default function () {
  const accountId = pickAccountId();
  const payload = JSON.stringify({
    transactionId: `tx-${accountId}-${__VU}-${__ITER}-${Date.now()}`,
    accountId: accountId,
    amount: Number((Math.random() * 100000).toFixed(2)),
    merchantCategory: 'RETAIL',
    country: 'KR',
    occurredAt: new Date().toISOString(),
  });

  const res = http.post(`${BASE_URL}/api/transactions`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    '202 accepted': (r) => r.status === 202,
  });

  sleep(0.05);
}
