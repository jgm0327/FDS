package com.fdsv2;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 이 스모크 테스트는 배선만 확인하고 실제 Kafka에는 절대 연결하지 않아야 한다 — 로컬에 다른
 * 프로젝트의 Kafka가 기본 포트(9092)에 떠 있으면 그쪽에 토픽을 만들어버리는 사고로 이어질 수 있다
 * (session-02에서 겪은 포트 충돌과 같은 종류의 문제, CP2 코드 리뷰에서 실제로 재현/발견됨).
 *
 * 세 겹으로 막는다:
 *   1) spring.kafka.streams.auto-startup=false — KafkaStreams 엔진 자체가 안 뜬다.
 *   2) spring.kafka.admin.auto-create=false — KafkaAdmin이 NewTopic 빈(transaction-events,
 *      account-feature-updates)을 자동 생성하는 걸 원천 차단한다. 처음엔 이 설정 없이 (1)번만
 *      걸었는데, KafkaStreams와 무관하게 KafkaAdmin이 그대로 토픽을 만들어버려서 사고가 재현됐다
 *      — 이게 실질적인 근본 수정이다.
 *   3) spring.kafka.bootstrap-servers=localhost:1 — 혹시 모를 다른 경로의 연결 시도까지 대비한
 *      방어선. 아무 서비스도 안 쓰는 포트라 연결 시도해도 즉시 거부되어 대기 시간도 없다.
 */
@SpringBootTest(properties = {
		"spring.kafka.streams.auto-startup=false",
		"spring.kafka.admin.auto-create=false",
		"spring.kafka.bootstrap-servers=localhost:1"
})
class FdsV2BackendApplicationTests {

	@Test
	void contextLoads() {
	}

}
