import http from 'k6/http';
import { check, sleep } from 'k6';

// CP3 — 온라인 피처 스토어 조회 부하 시나리오 (docs/PERFORMANCE_MEASUREMENT.md CP3 참고)
//
// 1) 먼저 계좌 풀(ACCOUNT_POOL_SIZE)만큼 거래를 발행해서 Redis에 피처를 채워 넣고
// 2) 그 계좌들을 반복 조회(GET /api/features/{accountId})해서 캐시 히트율/GET latency를 관측한다.
// 가끔 존재하지 않는 계좌를 섞어서 캐시 미스도 발생시킨다 (히트율이 100%로만 나오면 의미가 없음).
//
// GET /api/features/{accountId}는 CP4(모델 서빙)가 아직 없어서 이번 관측 확장 브랜치에서
// "측정 전용"으로 추가한 엔드포인트다 (FeatureQueryController 참고) — 진짜 서빙 로직이 아니다.
//
// 실행 예:
//   k6 run k6/cp3-feature-lookup-test.js

const BASE_URL = __ENV.BASE_URL || 'http://localhost:18080';
const ACCOUNT_POOL_SIZE = Number(__ENV.ACCOUNT_POOL_SIZE || 50);
const MISS_RATE = Number(__ENV.MISS_RATE || 0.1);

export const options = {
  scenarios: {
    // vus: 1로 고정 — shared-iterations에서 __ITER는 VU마다 0부터 다시 세는 로컬 카운터라,
    // VU를 여러 개 쓰면 accountId(__ITER % ACCOUNT_POOL_SIZE)가 계좌 풀 전체를 못 덮고 일부
    // 계좌에만 겹쳐서 채워진다 — 실제로 5 VU로 돌려봤다가 계좌 50개 중 10개에만 데이터가 들어가서
    // 캐시 미스율이 의도한 10%가 아니라 82%로 나오는 걸 확인하고 나서 1 VU로 고쳤다.
    warm_up: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: ACCOUNT_POOL_SIZE,
      exec: 'warmUp',
      maxDuration: '30s',
    },
    lookups: {
      executor: 'constant-vus',
      vus: 20,
      duration: '30s',
      exec: 'lookup',
      startTime: '5s', // warm_up이 끝날 시간을 감안한 여유
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.5'], // 의도적인 404(캐시 미스)도 섞여서 실패율 기준을 넉넉히 잡음
  },
};

function accountId(i) {
  return `acc-cp3-lookup-${i}`;
}

export function warmUp() {
  const id = accountId(__ITER % ACCOUNT_POOL_SIZE);
  const payload = JSON.stringify({
    transactionId: `warmup-${id}-${Date.now()}`,
    accountId: id,
    amount: Number((Math.random() * 1000).toFixed(2)),
    merchantCategory: 'RETAIL',
    country: 'KR',
    occurredAt: new Date().toISOString(),
  });
  http.post(`${BASE_URL}/api/transactions`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  sleep(0.1);
}

export function lookup() {
  const useMiss = Math.random() < MISS_RATE;
  const id = useMiss
    ? `acc-cp3-lookup-nonexistent-${Math.floor(Math.random() * 1000000)}`
    : accountId(Math.floor(Math.random() * ACCOUNT_POOL_SIZE));

  const res = http.get(`${BASE_URL}/api/features/${id}`);

  check(res, {
    '200 or 404': (r) => r.status === 200 || r.status === 404,
  });

  sleep(0.02);
}
