package com.fdsv2;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * spring.kafka.streams.auto-startup=false — CP2부터 컨텍스트에 @EnableKafkaStreams 빈이 포함되는데,
 * 기본값(auto-startup=true)이면 이 순수 배선 테스트조차 실제 브로커에 연결을 시도한다. 로컬에
 * 다른 프로젝트의 Kafka가 기본 포트(9092)에 떠 있으면 그쪽에 토픽을 만들어버리는 사고로 이어질 수
 * 있어서(session-02에서 겪은 포트 충돌과 같은 종류의 문제), 이 스모크 테스트는 배선만 확인하고
 * 실제 연결은 하지 않도록 막는다.
 */
@SpringBootTest(properties = "spring.kafka.streams.auto-startup=false")
class FdsV2BackendApplicationTests {

	@Test
	void contextLoads() {
	}

}
