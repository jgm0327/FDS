# 세션 로그

## 날짜 / 브랜치 / 세션 번호

2026-09-11, backend/model-client-grpc-benchmark, session-02

## 이번 세션에서 다룬 질문/요청

- PR #16을 `/code-review` high 강도로 리뷰 → 반영. (이번엔 리뷰 에이전트가 findings만 보고하고
  자동 수정은 적용하지 않은 채 끝나서, 지적된 내용을 직접 판단해서 고쳤다.)

## 변경/결정된 내용

10개 findings 중 실익이 있는 7개를 직접 반영, 나머지 3개는 근거를 남기고 보류:

1. **(가장 중요) `protocol` 오타/대소문자 값이면 빈이 하나도 안 만들어져 기동 자체가 실패**:
   `RestClientTorchServeHttpCaller`/`GrpcTorchServeHttpCaller`의 `@ConditionalOnProperty(
   havingValue="rest"/"grpc")` 조합은 "rest"/"grpc" 정확히 일치가 아니면(예: "REST", 오타)
   둘 다 비활성화된다. `@ConditionalOnExpression`으로 바꿔서 "정확히 grpc(대소문자 무관)가
   아니면 전부 REST"로 대칭을 재구성 — 오타가 있어도 최소한 기동은 되고 REST로 안전하게
   떨어진다.
2. **gRPC 데드라인이 재연결 비용까지 같은 예산 안에서 처리**: REST는 connect/read timeout을
   각각 timeout-ms만큼 따로 주는데, gRPC는 `withDeadlineAfter`가 RPC 전체(재연결 포함)를 한
   예산으로 묶는다. 완전한 비대칭 해소(연결/호출 데드라인 분리)는 gRPC가 기본 지원하지
   않아서, 대신 keepAlive(30초/5초/keepAliveWithoutCalls)로 유휴 중 연결이 끊겨 재연결이
   필요해지는 상황 자체를 줄이는 완화책을 택했다.
3. **`fallbackReason()`이 gRPC 실패를 전부 "torchserve_error"로 뭉갬**: REST 실패와 달리
   gRPC 실패는 `StatusRuntimeException`의 상태 코드(DEADLINE_EXCEEDED, UNAVAILABLE 등)로
   구체적 원인을 이미 갖고 있는데 버리고 있었다 — `grpc_<코드소문자>`로 태깅해서 REST와 같은
   수준의 세분화를 맞췄다. 회귀 테스트 추가.
4. **성공 경로만 테스트하고 있었음**: `GrpcTorchServeHttpCallerTest`에 (a) gRPC 레벨 오류
   (`Status.UNAVAILABLE`)가 예외로 전파되는지, (b) 성공 응답이지만 메시지 안의
   `status` 필드에 오류가 담긴 경우도 거르는지 테스트 추가.
5. **단항 Predictions 응답의 `status` 필드를 확인 안 하고 있었음**: 위 4번 테스트와 짝을
   이뤄, `response.hasStatus() && getCode() != 0`이면 명시적으로 예외를 던지도록 수정 — 안
   하면 오류가 "성공"으로 취급돼 빈/이상 바이트가 그대로 JSON 파싱 단계로 넘어가 원인이
   가려진다.
6. **Timer를 매 호출마다 새로 생성/등록**: `RestClientTorchServeHttpCaller`/
   `GrpcTorchServeHttpCaller` 둘 다 `call()` 안에서 `Timer.builder(...).register(...)`를
   매번 다시 했다 — 생성자에서 한 번만 만들어 필드로 캐싱하도록 수정.
7. **위 Timer 생성 로직이 두 클래스에 복붙돼 있었음**: `TorchServeCallerLatencyTimer`(신규,
   패키지 전용 헬퍼)로 지표명/description/histogram 설정을 한 곳으로 모아서, 6번 수정과 같이
   해결했다.
8. **protoc(4.35.0)과 protobuf-java(4.35.1) 버전이 주석의 주장("맞춰뒀다")과 실제로 한 자리
   어긋나 있었음**: protoc을 4.35.1로 정확히 맞춤(둘 다 존재하는 버전인지 Maven Central에서
   확인 후 적용).

**보류(근거 남김)**:
- gRPC 호스트/포트를 REST의 `base-url`에서 파생시키지 않고 별도 설정으로 둔 것 — 실제로
  파생시키려면 URI 파싱/기본값 로직이 추가로 필요한데, gRPC가 아직 옵션 기능(기본 비활성)이라
  이번엔 과설계로 판단해 보류.
- `@PreDestroy`의 우아한 종료(`shutdown()` + `awaitTermination` + 필요시 `shutdownNow()`)는
  이미 반영함(원래 findings에 있던 걸 6/7번과 같이 처리하며 포함).

## 설계 의도 및 트레이드오프

- **REST/gRPC 조건을 비대칭에서 대칭으로**: 원래 "rest면 A, 아니면(matchIfMissing) A"
  vs "grpc면 B"로 짜여있던 게, 사실상 "rest거나 grpc가 아닌 모든 것 = A"와 "정확히 grpc만
  B"로 겹치는 조건이었다(교집합에 결함). "정확히 grpc가 아니면 전부 A, 정확히 grpc면 B"로
  두 조건을 서로의 여집합으로 만들어야 "둘 다 비활성화"라는 상태 자체가 원천적으로 안 생긴다.
- **keepAlive는 근본 해결이 아니라 완화책임을 명시**: 재현 확인 결과, 이 완화책으로도 채널을
  "처음" 여는 순간의 연결 수립 비용 자체는 없어지지 않는다(당연히 최초 1회는 반드시 연결해야
  하므로) — 실제로 fix 적용 후 재기동한 앱에서도 첫 2회 호출이 여전히 `grpc_deadline_exceeded`
  로 폴백되고 3번째부터 성공하는 걸 확인했다. keepAlive가 막는 건 "한 번 연결된 채널이 유휴
  상태에서 끊겨서 나중에 또 재연결 비용이 드는" 상황이지, "앱이 막 뜬 시점의 최초 연결"이
  아니다 — k6 스크립트의 워밍업이 애초에 이 최초 비용을 흡수하도록 설계된 이유가 이거다.

## 확인 방법 (실제로 수행함)

```
./gradlew test   # 신규 테스트(gRPC 오류 전파 2건, fallbackReason 회귀 1건) 포함 전체 통과
```

- 실제 TorchServe(WSL) 대상으로 재기동 후 재검증: 처음 2회 호출은 예상대로
  `reason=grpc_deadline_exceeded`로 폴백(신규 태그가 정확히 찍히는 것 확인), 3~4번째 호출부터
  `source=MODEL`로 정상 복귀 — keepAlive 완화책이 "최초 연결 비용"까지는 없애지 않는다는 위
  트레이드오프 설명과 정확히 일치하는 실측.

## 다음에 이어서 할 일

- 머지.
- 보류한 2개(gRPC 호스트/포트 파생, 그 외 낮은 우선순위 항목)는 gRPC가 실제로 기본 경로가
  되는 시점에 재검토.
- session-01의 "다음에 이어서 할 일"(꼬리 지연 재검증에 전용 리소스 필요 등)은 그대로 유효.
