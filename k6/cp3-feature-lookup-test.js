import http from 'k6/http';
import { check, sleep } from 'k6';

// CP3 — 온라인 피처 스토어 조회 부하 시나리오 (docs/PERFORMANCE_MEASUREMENT.md CP3 참고)
//
// setup()에서 계좌 풀(ACCOUNT_POOL_SIZE)만큼 거래를 발행해서 Redis에 피처를 채우고, 실제로
// Redis에 반영됐는지 GET으로 폴링해서 확인한 뒤에야 lookups 시나리오를 시작한다. 그 계좌들을
// 반복 조회(GET /api/features/{accountId})해서 캐시 히트율/GET latency를 관측하고, 가끔 존재하지
// 않는 계좌를 섞어서 캐시 미스도 발생시킨다 (히트율이 100%로만 나오면 의미가 없음).
//
// GET /api/features/{accountId}는 CP4(모델 서빙)가 아직 없어서 이번 관측 확장 브랜치에서
// "측정 전용"으로 추가한 엔드포인트다 (FeatureQueryController 참고) — 진짜 서빙 로직이 아니고,
// 기본적으로 꺼져 있다. 실행 전에 아래처럼 켜야 한다:
//
//   FDS_FEATURE_STORE_QUERY_ENDPOINT_ENABLED=true ./gradlew bootRun
//   k6 run k6/cp3-feature-lookup-test.js
//
// 처음엔 "워밍업 시나리오 + 5초 대기" 방식으로 했는데, Kafka Streams -> Redis 싱크까지 이어지는
// 비동기 파이프라인이 5초 안에 못 끝나면 lookups가 아직 안 채워진 계좌를 때려서 미스율이
// 의도(10%)보다 훨씬 높게 부풀어 오르는 문제가 있었다(코드 리뷰 지적) — setup()에서 폴링으로
// "진짜 준비됐는지" 확인하는 방식으로 바꿔서 근본적으로 해결했다.

const BASE_URL = __ENV.BASE_URL || 'http://localhost:18080';
const ACCOUNT_POOL_SIZE = Number(__ENV.ACCOUNT_POOL_SIZE || 50);
const MISS_RATE = Number(__ENV.MISS_RATE || 0.1);
const WARMUP_TIMEOUT_MS = 20000;

export const options = {
  scenarios: {
    lookups: {
      executor: 'constant-vus',
      vus: 20,
      duration: '30s',
      exec: 'lookup',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.5'], // 의도적인 404(캐시 미스)도 섞여서 실패율 기준을 넉넉히 잡음
  },
};

function accountId(i) {
  return `acc-cp3-lookup-${i}`;
}

// setup()은 k6가 시나리오를 시작하기 전에 딱 한 번 실행한다 — 여기서 계좌 풀을 채우고, Redis에
// 실제로 반영됐는지 확인이 끝난 뒤에만 아래 lookup() 시나리오가 시작되게 한다.
export function setup() {
  for (let i = 0; i < ACCOUNT_POOL_SIZE; i++) {
    const id = accountId(i);
    const payload = JSON.stringify({
      transactionId: `warmup-${id}`,
      accountId: id,
      amount: Number((Math.random() * 1000).toFixed(2)),
      merchantCategory: 'RETAIL',
      country: 'KR',
      occurredAt: new Date().toISOString(),
    });
    http.post(`${BASE_URL}/api/transactions`, payload, {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const deadline = Date.now() + WARMUP_TIMEOUT_MS;
  let allReady = false;
  while (Date.now() < deadline && !allReady) {
    allReady = true;
    for (let i = 0; i < ACCOUNT_POOL_SIZE; i++) {
      const res = http.get(`${BASE_URL}/api/features/${accountId(i)}`);
      if (res.status !== 200) {
        allReady = false;
        break;
      }
    }
    if (!allReady) {
      sleep(0.5);
    }
  }
  if (!allReady) {
    console.warn(
      `${WARMUP_TIMEOUT_MS}ms 안에 계좌 풀 전체가 Redis에 반영되지 않음 — 미스율이 의도(${MISS_RATE * 100}%)보다 높게 나올 수 있음`
    );
  }
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
