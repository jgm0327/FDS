# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-11, backend/model-client-grpc-benchmark, session-01

## 이번 세션에서 다룬 질문/요청

- "은행은 REST를 쓸 텐데 gRPC와 비교하는 게 경쟁력이 될까" — 실제로는 조직마다 다르고,
  "무엇을 골랐냐"가 아니라 "실측으로 트레이드오프를 판단했다"가 경쟁력이라는 논의 끝에,
  `docs/BACKEND.md`/`docs/ARCHITECTURE.md`에 이미 남아있던 TODO("TorchServe는 REST 먼저,
  gRPC는 다음 단계")를 실제로 구현/실측하기로 결정.
- 정식 구현 + 실측(가장 큰 범위)으로 진행 승인받음.

## 변경/결정된 내용

- `src/main/proto/inference.proto`: TorchServe 공식 gRPC 추론 API 정의를 그대로 가져옴(서버가
  아니라 클라이언트 스텁만 필요).
- `build.gradle`: `com.google.protobuf` 그래들 플러그인 + `io.grpc:grpc-{netty-shaded,protobuf,stub}`
  의존성 추가. 버전을 명시적으로 1.83.1/protobuf-java 4.35.1로 맞춘 이유는 "막혔던 문제" 참고.
- `GrpcTorchServeHttpCaller`(신규): 기존 `TorchServeHttpCaller` 인터페이스를 그대로 구현 —
  `TorchServeModelInferenceClient`(Circuit Breaker/Bulkhead/폴백 로직)를 전혀 안 건드리고
  전송 계층만 REST/gRPC로 교체 가능하게 만들었다. `fds.model-serving.torchserve.protocol`
  (rest 기본값/grpc)로 어느 쪽이 활성화될지 결정(`@ConditionalOnProperty`).
- `RestClientTorchServeHttpCaller`: 위 조건부 빈 전환에 맞춰 `@ConditionalOnProperty`
  추가(matchIfMissing=true로 기존 동작 100% 유지). REST/gRPC를 동일 지표명
  (`fds.model-serving.torchserve.caller.latency`, protocol 태그)으로 비교할 수 있도록 둘 다에
  Micrometer Timer를 직접 추가.
- `GrpcTorchServeHttpCallerTest`(신규): 실제 TorchServe 없이 로컬 임시 포트에 가짜
  `InferenceAPIsService` 서버를 띄워, 요청이 "data" 키에 JSON을 담아 보내지는지/응답 바이트를
  올바르게 디코딩하는지 검증.
- `k6/cp4-grpc-vs-rest-latency-test.js`(신규): `cp4-model-client-fault-injection-test.js`의
  워밍업 패턴을 재사용하되, 장애 주입 없이 순수 latency 비교 목적. VUS 기본값을 4로 맞춰
  TorchServe 워커 수/Bulkhead 한도와 일치시켜(둘 다 4) 동시성 제한이 결과에 섞이지 않게 함.
- `ai/config.properties`(신규): 로컬 실측용 TorchServe 설정(REST/gRPC 포트, 바인딩 주소).
- `docs/ARCHITECTURE.md`/`ai/README.md`: 방법론 + 실측 결과 + 최종 결론(REST 유지) 반영.

## 설계 의도 및 트레이드오프

- **인터페이스 재사용으로 위험 최소화**: gRPC 도입이라는 큰 변경을 하면서도, Circuit
  Breaker/Bulkhead/폴백처럼 이미 실측/버그수정을 거친 로직(`TorchServeModelInferenceClient`)은
  하나도 건드리지 않았다 — `TorchServeHttpCaller`라는 좁은 인터페이스 뒤에 전송 계층만
  갈아끼우는 설계 덕분. 회귀 위험이 "새 캐스터 클래스 하나"로 국한된다.
- **REST/gRPC를 같은 앱, 같은 지표로 비교**: 별도 벤치마크 프로젝트를 만드는 대신 실제 앱의
  실제 경로(Circuit Breaker/Bulkhead까지 포함한 실제 운영 코드 경로)에서 설정값 하나만 바꿔
  비교했다 — "이상적인 조건"이 아니라 "실제로 배포됐을 때의 조건"을 재는 게 목적이었기 때문.
- **비교 대상은 "호출 자체"로 좁힘**: Redis 조회, Circuit Breaker/Bulkhead 오버헤드, 규칙 엔진
  연산 등은 REST/gRPC 어느 쪽을 쓰든 동일하므로 비교에서 의미가 없다 — 그래서 각 Caller
  구현체 안에서 직접 Timer로 감싸(`EnsembleFraudDecisionService`가 앙상블 결합 연산만 재는
  것과 같은 원칙), "직렬화+네트워크 왕복"만 순수하게 비교했다.

## 막혔던 문제와 해결 방법

- **(가장 오래 걸림) Git Bash(MSYS)가 WSL로 넘기는 절대경로 인자를 자기 멋대로 Windows
  경로로 바꿔버림**: `wsl.exe -- bash -c "/root/venv/bin/pip install ..."`이 매번
  `bash: line 1: C:/Program: No such file or directory`로 실패. 처음엔 pip/torchserve
  자체의 문제로 오인하고 한참 헤맸다 — `env -i`로 환경변수를 하나씩 지워가며 원인을 좁히다가,
  `/bin/bash`라는 인자 자체가 `C:/Program Files/Git/usr/bin/bash`로 둔갑해 있는 걸 발견했다.
  Git Bash(MSYS2)가 "POSIX 절대경로처럼 보이는 인자를 네이티브 Windows 실행파일(wsl.exe)에
  넘길 때 자동으로 Windows 경로로 변환"하는 알려진 동작 — `MSYS_NO_PATHCONV=1`을 앞에 붙여서
  이 자동 변환을 껐다. 이후 모든 `wsl.exe` 호출에 이 환경변수를 붙였다.
  - 부작용: 이 문제 때문에 처음 시도한 `pip install torchserve torch-model-archiver` 자체가
    조용히 실패(`2>&1 | tail -30`로 파이프했더니 `tail`의 종료 코드 0이 전체 명령 종료 코드로
    보고돼, 백그라운드 작업이 "성공"으로 잘못 표시됐다) — 나중에 `torch-model-archiver: No
    such file`로 뒤늦게 발견. 이후로는 파이프 없이 원본 명령 종료 코드를 직접 확인하는 습관으로
    바꿨다.
- **gRPC 코드 생성 스텁이 런타임에 `AbstractMethodError`**: `protoc-gen-grpc-java:1.68.1` +
  `protoc:3.25.5`로 생성했는데, Spring Boot 4.1.1의 관리 의존성이 io.grpc/protobuf-java를
  이미 1.83.1/4.35.1로 강제 승격시키고 있었다 — "선언한 버전"과 "실제로 도는 버전"이 어긋나서
  생긴 ABI 불일치. `./gradlew dependencyInsight`로 실제 승격 버전을 확인하고, 선언/코드생성
  플러그인 버전을 전부 1.83.1/4.35.1(과 호환되는 protoc 4.35.0)으로 통일해서 해결.
- **TorchServe가 Java를 못 찾음**: WSL엔 파이썬만 있고 JRE가 없었다 — `openjdk-17-jre-headless`
  설치로 해결.
- **`torchserve --start`로 띄운 프로세스가 다음 `wsl.exe` 호출부터 사라짐**: TorchServe의
  자체 데몬화(fork)가, `wsl.exe -- bash -c "..."` 호출이 끝나는 시점에 WSL이 그 호출에 속한
  프로세스 트리를 정리하면서 같이 죽는 것으로 보였다. `--foreground`로 TorchServe 자체
  데몬화를 끄고, `setsid nohup ... </dev/null >log 2>&1 & disown`으로 완전히 분리해서 별도
  `wsl.exe` 호출 사이에도 살아남게 만들었다.
- **REST/gRPC 둘 다 기본이 127.0.0.1 바인딩이라 Windows 앱이 못 붙음**: WSL2 localhost
  포워딩이 모든 바인딩 형태를 커버하지 않았다 — `config.properties`에 `inference_address=
  http://0.0.0.0:...`, `grpc_inference_address=0.0.0.0`을 명시(gRPC는 REST와 별개 속성이라는
  걸 TorchServe 소스(`ConfigManager.java`)를 직접 읽고 확인)하고, Windows 쪽 앱은
  `wsl hostname -I`로 얻은 IP로 접근하도록 바꿨다.
- **gRPC 첫 호출이 채널 연결 지연으로 타임아웃(300ms) 초과**: `ManagedChannel`이 지연 연결이라
  첫 RPC에 연결 수립 비용이 얹힌다 — k6 스크립트의 `setup()` 워밍업이 실제 측정 구간 전에
  이 비용을 흡수하게 설계돼 있어서(기존 cp4-model-client-fault-injection-test.js 패턴 재사용)
  실제 측정에는 영향 없었다.

## 확인 방법 (실제로 수행함)

```
./gradlew test              # GrpcTorchServeHttpCallerTest 포함 전체 통과
k6 run k6/cp4-grpc-vs-rest-latency-test.js   # REST/gRPC 각 2회 반복(60s + 45s), VUS=4
```

Prometheus(`fds.model-serving.torchserve.caller.latency`, protocol 태그)로 확인한 p50/p95/p99
(단위 ms):

| 프로토콜 | 실행 | n | p50 | p95 | p99 |
|---|---|---|---|---|---|
| REST | 1회차(60s) | 1701 | 31.0 | 75.3 | 127.7 |
| gRPC | 1회차(60s) | 1631 | 23.0 | 144.8 | 270.5 |
| gRPC | 1+2회차 누적(45s 추가) | 2830 | 25.5 | 151.1 | 244.0 |
| REST | 2회차(45s, 단독) | 1152 | 38.6 | 148.8 | 281.2 |

**정직한 결론**: p50(중앙값)은 gRPC가 두 번의 측정 모두에서 REST보다 낮았다(23~26ms vs
31~39ms) — 바이너리 프로토콜(protobuf) + HTTP/2가 JSON + HTTP/1.1보다 유리하다는 통상적
설명과 방향이 일치하고, 재현성도 있었다. 반면 p95/p99(꼬리 지연)는 **REST 자체도 두 번의
실행에서 결과가 거의 2배 차이 났다**(p95 75ms→149ms) — 이건 REST vs gRPC 차이가 아니라 이
측정 환경(WSL2+Docker+다른 프로세스가 같이 도는 공유 개발 머신) 자체의 노이즈가 신호보다
크다는 뜻이다. 그래서 "gRPC가 꼬리 지연에서 REST보다 나쁘다"는 결론은 내리지 않았다 — 데이터가
그렇게 말하지 않는다.

## 다음에 이어서 할 일

- 코드 리뷰 후 머지.
- 꼬리 지연(p95/p99) 비교를 신뢰할 수 있으려면 전용 리소스(공유 없는 머신) + 훨씬 많은 반복
  측정이 필요하다 — 지금은 결론을 못 내린 채로 남겨둔다.
- REST를 기본값으로 유지하되 `GrpcTorchServeHttpCaller`는 옵션으로 남김 — 향후 실제 운영
  트래픽에서 p50 개선이 유의미해지는 시점에 재검토.
- TorchServe/Docker WSL 환경(`/root/fds-ai-venv`, `MSYS_NO_PATHCONV=1` 습관)은 앞으로도 재사용
  가능 — `ai/README.md`에 반영해 둠.
