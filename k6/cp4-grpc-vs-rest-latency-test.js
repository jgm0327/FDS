import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

// (backend/model-client-grpc-benchmark) TorchServe 호출 경로(REST vs gRPC) 실측 비교용 부하.
//
// cp4-model-client-fault-injection-test.js와 달리 장애 주입은 하지 않는다 — 목적이 다르다.
// 여기서는 "TorchServe가 정상 응답하는 동안, 호출 자체(직렬화+네트워크 왕복)에 걸리는 시간이
// REST와 gRPC에서 얼마나 다른가"만 본다. 그래서:
//   - VUS 기본값을 4로 맞췄다 — fds.model-serving.torchserve.bulkhead.max-concurrent-calls
//     기본값(4)과 TorchServe 워커 수(이 실측에서 4로 띄움)에 맞춰서, Bulkhead 거절/워커 큐잉
//     대기시간이 섞여 들어가는 걸 피한다(섞이면 "전송 방식 차이"가 아니라 "동시성 한도 차이"를
//     재는 꼴이 된다).
//   - source가 계속 MODEL인지를 카운터로 지켜본다 — FALLBACK이 하나라도 나오면 그 구간은
//     TorchServe 호출이 실제로 안 됐다는 뜻이라 latency 비교에서 제외해야 한다.
//
// 실행 예 (REST):
//   FDS_MODEL_SERVING_QUERY_ENDPOINT_ENABLED=true TORCHSERVE_PROTOCOL=rest SERVER_PORT=18080 \
//     KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_REDIS_PORT=16379 ./gradlew bootRun
//   k6 run k6/cp4-grpc-vs-rest-latency-test.js
//
// 실행 예 (gRPC, 다른 터미널에서 앱만 재기동):
//   ... TORCHSERVE_PROTOCOL=grpc ... ./gradlew bootRun
//   k6 run k6/cp4-grpc-vs-rest-latency-test.js
//
// p50/p95/p99는 k6 콘솔이 아니라 Prometheus에서 확인한다(REST/gRPC 양쪽 다 같은 앱 타이머
// fds.model-serving.torchserve.caller.latency를 protocol 태그로 나눠서 쌓으므로):
//   histogram_quantile(0.95, rate(fds_model_serving_torchserve_caller_latency_seconds_bucket{protocol="rest"}[2m]))
//   histogram_quantile(0.95, rate(fds_model_serving_torchserve_caller_latency_seconds_bucket{protocol="grpc"}[2m]))

const BASE_URL = __ENV.BASE_URL || 'http://localhost:18080';
const ACCOUNT_POOL_SIZE = Number(__ENV.ACCOUNT_POOL_SIZE || 20);
const RUN_DURATION_SECONDS = Number(__ENV.RUN_DURATION_SECONDS || 60);
const VUS = Number(__ENV.VUS || 4);
const WARMUP_TIMEOUT_MS = 20000;

const modelResponses = new Counter('fds_source_model_responses');
const fallbackResponses = new Counter('fds_source_fallback_responses');

export const options = {
  scenarios: {
    fraud_score_lookups: {
      executor: 'constant-vus',
      vus: VUS,
      duration: `${RUN_DURATION_SECONDS}s`,
      exec: 'lookup',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

function accountId(i) {
  return `acc-cp4-grpc-bench-${i}`;
}

// cp4-model-client-fault-injection-test.js와 동일한 워밍업 — 계좌마다 시간 간격을 벌린 정상
// 거래 3건을 발행해 "정상 패턴" 시퀀스를 만들어 둔다.
export function setup() {
  const now = Date.now();
  const offsetsMinutes = [-120, -90, -30];

  for (let i = 0; i < ACCOUNT_POOL_SIZE; i++) {
    const id = accountId(i);
    offsetsMinutes.forEach((offsetMinutes, step) => {
      const occurredAt = new Date(now + offsetMinutes * 60 * 1000).toISOString();
      const payload = JSON.stringify({
        transactionId: `warmup-${id}-${step}`,
        accountId: id,
        amount: 100 + step,
        merchantCategory: 'GROCERY',
        country: 'KR',
        occurredAt,
      });
      http.post(`${BASE_URL}/api/transactions`, payload, {
        headers: { 'Content-Type': 'application/json' },
      });
    });
  }

  const deadline = Date.now() + WARMUP_TIMEOUT_MS;
  let allReady = false;
  while (Date.now() < deadline && !allReady) {
    allReady = true;
    for (let i = 0; i < ACCOUNT_POOL_SIZE; i++) {
      const res = http.get(`${BASE_URL}/api/fraud-score/${accountId(i)}`);
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
    console.warn(`${WARMUP_TIMEOUT_MS}ms 안에 계좌 풀 워밍업이 끝나지 않음 — 초반 결과가 불안정할 수 있음`);
  }

  console.log(`워밍업 완료. ${RUN_DURATION_SECONDS}초간 VUS=${VUS}로 부하를 겁니다.`);
}

export function lookup() {
  const id = accountId(Math.floor(Math.random() * ACCOUNT_POOL_SIZE));
  const res = http.get(`${BASE_URL}/api/fraud-score/${id}`);

  const ok = check(res, {
    '200 응답': (r) => r.status === 200,
  });

  if (ok) {
    try {
      const body = JSON.parse(res.body);
      if (body.source === 'MODEL') {
        modelResponses.add(1);
      } else if (body.source === 'FALLBACK') {
        fallbackResponses.add(1);
      }
    } catch (e) {
      // 파싱 실패는 무시 — http_req_failed 임계값이 실패 자체는 잡아준다.
    }
  }

  sleep(0.1);
}
