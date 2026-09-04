# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-04, backend/redis-observability, session-01

## 이번 세션에서 다룬 질문/요청

- "패널 확장해줘" — CP1(Kafka), CP2(Kafka Streams) 관측 확장에 이어 CP3(Redis)도 같은 패턴으로
  `docs/PERFORMANCE_MEASUREMENT.md` CP3 표(GET/SET latency, 캐시 히트율, TTL 만료, 메모리 사용량)를
  Grafana에 연결.

## 변경/결정된 내용

- `docker-compose.yml`: `oliver006/redis_exporter` 서비스 추가 (호스트 포트 19121).
- `monitoring/prometheus/prometheus.yml`: `redis-exporter` 스크레이프 잡 추가.
- `monitoring/grafana/provisioning/dashboards/json/cp3-redis-feature-store.json` 신규 — 4개 패널
  (GET/SET latency p50/p99, 캐시 히트율, TTL 만료 건수, 메모리 사용량).
- `com.fdsv2.featurestore.FeatureQueryController` 신규 — `GET /api/features/{accountId}` (측정 전용,
  아래 "설계 의도" 참고).
- `k6/cp3-feature-lookup-test.js` 신규 — 계좌 풀 워밍업(POST) 후 반복 조회(GET)로 캐시 히트율/GET
  latency 신호 생성, 10% 확률로 존재하지 않는 계좌를 섞어 미스도 발생.
- `FeatureQueryControllerTest` 단위 테스트 2개 추가.

## 설계 의도 및 트레이드오프

- **CP3에서 명시적으로 미뤘던 GET 엔드포인트를 이번엔 "측정 전용"으로 추가**: CP3 구현 세션의
  핵심 설계 결정 중 하나가 "읽기(GET) API는 CP4(모델 서빙)의 역할이라 이번 범위에서 안 만듦"이었다.
  그런데 GET latency/캐시 히트율은 애초에 GET을 하는 컴포넌트가 하나도 없으면 관측 자체가
  불가능하다 — 그래서 이번 관측 확장 브랜치에서 딱 "Redis GET 한 번"만 하는 최소 엔드포인트를
  추가하고, 코드 주석/문서에 "CP4가 진짜 서빙 로직으로 이 자리를 대체/확장할 예정, 캐싱/재시도/
  타임아웃 등 실제 서빙 관심사는 전혀 안 다룸"이라고 명확히 선을 그었다. 이전 결정을 뒤집는
  게 아니라, "관측을 위한 최소 도구"와 "실제 서빙 로직"을 구분한 것.
- **`redis_exporter`의 커맨드 통계는 별도 플래그 없이 기본 제공**: 처음엔 `--collect-command-stats`
  같은 플래그가 필요할 거라 예상하고 시도했는데 존재하지 않는 플래그라 컨테이너가 즉시 죽었다.
  `-h`로 도움말을 확인해보니 `-exclude-latency-histogram-metrics`(끄는 플래그)가 있다는 건 기본이
  켜져 있다는 뜻이었다 — 실측으로 알아낸 사실.
- **p95 대신 p50/p99/p99.9**: `redis_exporter`가 실제로 노출하는 분위수가 이 3개뿐이라(Redis
  자체 `LATENCY HISTOGRAM` 명령 기반), CP3 표의 "p50/p95/p99" 문구와 다르지만 이 값들로 대체.

## 막혔던 문제와 해결 방법

1. **Prometheus/Grafana가 다른 worktree의 docker-compose로 떠 있어서, 이 브랜치의 설정 변경을
   바로 검증할 수 없었음**: 이전 CP2 관측 확장 세션과 같은 패턴으로, 검증용으로 실행 중인 스택의
   마운트 경로에 파일을 임시 복사해서 확인한 뒤(prometheus.yml, 대시보드 JSON), 검증이 끝나고
   그 worktree는 원래 상태로 되돌렸다(`git checkout --`).
2. **`docker compose up -d redis-exporter`가 "container name already in use" 충돌로 실패**:
   `fds-v2-redis`가 이미 다른 compose 프로젝트(`fds-redis-feature-store`)에서 떠 있는 상태라,
   이 브랜치의 compose 파일이 같은 이름으로 재생성을 시도하면서 충돌났다. `docker run`으로
   redis-exporter 컨테이너를 직접 그 네트워크에 붙여서 검증하고, `docker network connect`로
   Prometheus가 있는 네트워크에도 별칭(`redis-exporter`)을 붙여 스크레이프가 가능하게 했다 — 정식
   `docker compose up` 경로는 docker-compose.yml에 그대로 남겨뒀다(클린 환경에서는 문제없이 동작).
3. **k6 `http_req_failed`가 의도한 10%가 아니라 82%로 나옴**: `shared-iterations`의 `__ITER`가
   VU마다 0부터 다시 세는 로컬 카운터라는 걸 몰라서, 5 VU로 워밍업을 돌리면 계좌 50개 중 10개에만
   데이터가 몰려 쓰였다. 1 VU로 고쳐서 계좌 풀 전체를 정확히 한 번씩 채우도록 수정 — 이후 실제
   미스율이 9.87%로 의도(10%)와 거의 일치.

## 확인 방법 (실제로 수행함)

```
./gradlew test                              # 신규 2개 포함 전체 통과
FDS_FEATURE_STORE_TTL_MINUTES=1 \
SERVER_PORT=18080 KAFKA_BOOTSTRAP_SERVERS=localhost:19092 FDS_KAFKA_REPLICATION_FACTOR=1 \
  FDS_REDIS_PORT=16379 ./gradlew bootRun
k6 run k6/cp3-feature-lookup-test.js
```

- `redis_latency_percentiles_usec`로 GET p50 ≈1µs, p99 ≈8µs, SET도 유사한 수준 확인.
- 캐시 히트율이 트래픽 구간에서 ~90%로 관측 — 의도한 미스율 10%와 정확히 일치.
- `redis_expired_keys_total`이 61건 증가 — TTL을 1분으로 짧게 오버라이드해서 실제 만료가
  일어나는 걸 짧은 시간 안에 관찰(운영 기본값은 그대로 30분).
- `redis_memory_used_bytes`가 계좌 생성/만료에 따라 1.27~1.31 MiB 사이에서 자연스럽게 변동.
- Grafana `FDS v2 - CP3 Redis Feature Store` 대시보드 4개 패널 전부 실데이터 렌더링 확인,
  스크린샷 저장(`docs/performance-results/2026-09-04_cp3-redis-dashboard.jpg`).

## 다음에 이어서 할 일

- PR 리뷰(`/code-review`) 후 머지.
- CP4(PyTorch 모델 서빙) 착수 여부 논의 — 착수 시 `FeatureQueryController`를 실제 서빙 로직으로
  대체/확장.
