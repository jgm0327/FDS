import http from 'k6/http';
import { check, sleep } from 'k6';

// CP5 — 판정 파이프라인 혼합 부하 시나리오 (docs/PERFORMANCE_MEASUREMENT.md CP5 참고)
//
// "실제 트래픽 패턴과 유사한 혼합 시나리오(정상 90% + 이상 패턴 10%)"를 그대로 구현한다.
// 계좌 풀(ACCOUNT_POOL_SIZE)마다 고정된 "평소 패턴"(국가, 평소 금액대)을 부여하고, 매 반복마다
// ANOMALY_RATE 확률로 그 패턴에서 크게 벗어나는(금액 30~50배 급증 + 국가 변경) 거래를 섞는다.
//
// CP5는 GET 엔드포인트를 호출할 필요 없이 완전히 자동으로 트리거된다(FraudDecisionEventListener,
// backend/decision-ensemble) — 이 스크립트는 부하만 만들고, 실제 판정 결과(액션 분포, e2e
// latency)는 Grafana "FDS v2 - CP5 Decision Ensemble" 대시보드에서 확인한다.
//
// 참고(2026-09-10 e2e 검증 세션 로그): "평소 금액"이 전체 기간 누적 평균이라, 같은 계좌가 이상
// 거래를 여러 번 맞으면 평균 자체가 뛰어올라 다음 이상 거래는 오히려 하드룰을 피해가기 쉬워진다
// (다음 개선 후보로 별도 문서화됨). 이 스크립트는 그 한계를 고치는 게 아니라, 지금 로직 그대로
// 혼합 트래픽에서 액션 분포/latency가 어떻게 나오는지 있는 그대로 관측하는 것이 목적이다.
//
// 실행:
//   docker compose up -d
//   SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 \
//     FDS_REDIS_PORT=16379 ./gradlew bootRun
//   k6 run k6/cp5-decision-mixed-test.js

const BASE_URL = __ENV.BASE_URL || 'http://localhost:18080';
const ACCOUNT_POOL_SIZE = Number(__ENV.ACCOUNT_POOL_SIZE || 200);
const ANOMALY_RATE = Number(__ENV.ANOMALY_RATE || 0.1);

const COUNTRIES = ['KR', 'US', 'JP', 'DE', 'VN'];

function homeCountry(i) {
  return COUNTRIES[i % COUNTRIES.length];
}

function normalAmount(i) {
  // 계좌마다 평소 금액대를 다르게 잡는다 — 전부 같은 금액이면 "평소 대비 배율"(amountRatio)이라는
  // 신호 자체가 무의미해진다.
  return 10000 + (i % 10) * 10000;
}

function anomalyCountry(i) {
  const home = homeCountry(i);
  const candidates = COUNTRIES.filter((c) => c !== home);
  return candidates[i % candidates.length];
}

export const options = {
  scenarios: {
    mixed_traffic: {
      executor: 'constant-vus',
      vus: 20,
      duration: '1m',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const i = Math.floor(Math.random() * ACCOUNT_POOL_SIZE);
  const accountId = `acc-cp5-mixed-${i}`;
  const isAnomaly = Math.random() < ANOMALY_RATE;

  const payload = isAnomaly
    ? {
        transactionId: `tx-${accountId}-${__VU}-${__ITER}-anomaly`,
        accountId,
        amount: Number((normalAmount(i) * (30 + Math.random() * 20)).toFixed(2)),
        merchantCategory: 'CASH_ADVANCE',
        country: anomalyCountry(i),
        occurredAt: new Date().toISOString(),
      }
    : {
        transactionId: `tx-${accountId}-${__VU}-${__ITER}`,
        accountId,
        amount: Number((normalAmount(i) * (0.8 + Math.random() * 0.4)).toFixed(2)),
        merchantCategory: 'GROCERY',
        country: homeCountry(i),
        occurredAt: new Date().toISOString(),
      };

  const res = http.post(`${BASE_URL}/api/transactions`, JSON.stringify(payload), {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    '202 accepted': (r) => r.status === 202,
  });

  sleep(0.1);
}
