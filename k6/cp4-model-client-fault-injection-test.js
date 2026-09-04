import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

// CP4 — 모델 서빙 호출 + 장애 주입 시나리오 (docs/PERFORMANCE_MEASUREMENT.md CP4 참고)
//
// "정상 부하 테스트 후, 모델 서버에 인위적 지연/오류를 주입하여 Circuit Breaker가 설계한
// 타임아웃대로 열리고 폴백으로 전환되는지 확인" — 이 장애 주입 자체(TorchServe 강제 종료)는
// k6가 제어할 수 있는 대상이 아니라서(별도 프로세스/컨테이너), CP3의 TTL 오버라이드 검증과
// 같은 패턴으로 "k6는 부하만 계속 걸고, 장애 주입은 운영자가 스크립트 실행 중 수동으로 수행"
// 하는 방식을 쓴다. RUN_DURATION_SECONDS 동안 계속 조회하면서 언제 FALLBACK으로 전환됐는지를
// 커스텀 카운터(modelResponses/fallbackResponses)로 집계해서 마지막 요약에 남긴다.
//
// GET /api/fraud-score/{accountId}는 CP5(판정 및 대응)가 나오기 전까지의 측정 전용 엔드포인트다
// (FraudScoreController 참고) — 기본 꺼져 있으니 실행 전에 켜야 한다:
//
//   FDS_MODEL_SERVING_QUERY_ENDPOINT_ENABLED=true SERVER_PORT=18080 \
//     TORCHSERVE_BASE_URL=http://localhost:8080 ./gradlew bootRun
//   (다른 터미널) TorchServe 기동 — ai/README.md "TorchServe 배포" 절 참고
//   k6 run k6/cp4-model-client-fault-injection-test.js
//
// 스크립트 실행 중 RUN_DURATION_SECONDS의 절반 지점 즈음에 다른 터미널에서
//   torchserve --stop
// 을 실행하면, 이후 요청들의 source가 MODEL -> FALLBACK으로 전환되는 걸 콘솔 요약과
// Grafana(FDS v2 - CP4 Model Client)에서 함께 확인할 수 있다.

const BASE_URL = __ENV.BASE_URL || 'http://localhost:18080';
const ACCOUNT_POOL_SIZE = Number(__ENV.ACCOUNT_POOL_SIZE || 20);
const RUN_DURATION_SECONDS = Number(__ENV.RUN_DURATION_SECONDS || 60);
const WARMUP_TIMEOUT_MS = 20000;

const modelResponses = new Counter('fds_source_model_responses');
const fallbackResponses = new Counter('fds_source_fallback_responses');

export const options = {
  scenarios: {
    fraud_score_lookups: {
      executor: 'constant-vus',
      vus: 10,
      duration: `${RUN_DURATION_SECONDS}s`,
      exec: 'lookup',
    },
  },
  thresholds: {
    // MODEL/FALLBACK 둘 다 200을 내야 정상 — CP3처럼 의도된 4xx가 없으므로 엄격하게 잡는다.
    http_req_failed: ['rate<0.05'],
  },
};

function accountId(i) {
  return `acc-cp4-fault-${i}`;
}

// 계좌마다 시간 간격을 벌린 정상 거래 3건을 발행해서 "정상 패턴" 시퀀스로 워밍업한다.
// (세션 로그 참고: 거래 간격을 안 벌리면 gapSec=0이 되어 burst 이상 패턴처럼 보인다.)
export function setup() {
  const now = Date.now();
  const offsetsMinutes = [-120, -90, -30]; // 2시간 전 / 1시간30분 전 / 30분 전

  for (let i = 0; i < ACCOUNT_POOL_SIZE; i++) {
    const id = accountId(i);
    offsetsMinutes.forEach((offsetMinutes, step) => {
      const occurredAt = new Date(now + offsetMinutes * 60 * 1000).toISOString();
      const payload = JSON.stringify({
        transactionId: `warmup-${id}-${step}`,
        accountId: id,
        amount: 100 + step, // 평소 대비 배율이 1 근처가 되도록 소폭만 변화
        merchantCategory: 'GROCERY',
        country: 'KR',
        occurredAt,
      });
      http.post(`${BASE_URL}/api/transactions`, payload, {
        headers: { 'Content-Type': 'application/json' },
      });
    });
  }

  // Redis :recent 리스트에 3건 다 반영됐는지 fraud-score 조회가 200을 낼 때까지 폴링.
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

  console.log(
    `워밍업 완료. ${RUN_DURATION_SECONDS}초간 부하를 겁니다 — ` +
      `절반 지점(약 ${Math.floor(RUN_DURATION_SECONDS / 2)}초) 즈음에 다른 터미널에서 ` +
      `'torchserve --stop'을 실행해 Circuit Breaker 전환을 관찰하세요.`
  );
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
      // 응답 파싱 실패는 그냥 건너뜀 — http_req_failed 임계값이 이미 실패 자체는 잡아준다.
    }
  }

  sleep(0.1);
}
