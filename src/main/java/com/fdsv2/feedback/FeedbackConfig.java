package com.fdsv2.feedback;

import java.util.Random;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * CP6 스케줄링 활성화 + {@link SimulatedLabelHeuristic}이 쓰는 {@link Random} 빈.
 *
 * {@code Random}을 별도 빈으로 뺀 이유: 테스트에서 시드를 고정하거나 결과를 예측 가능하게
 * 모킹하려면 생성자 주입이 필요하기 때문 — {@code SimulatedLabelHeuristicTest} 참고.
 */
@Configuration
@EnableScheduling
public class FeedbackConfig {

    @Bean
    public Random feedbackLabelRandom() {
        return new Random();
    }
}
