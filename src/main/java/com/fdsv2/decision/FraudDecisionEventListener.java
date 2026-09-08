package com.fdsv2.decision;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdsv2.featurestore.FeatureStoreUpdatedEvent;
import com.fdsv2.modelclient.RawFeatureStep;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * CP5 자동 판정 트리거 — 요구사항 "거래 이벤트 → 자동 판정까지 실제로 이어지도록 트리거 배선"에
 * 대응한다. CP3({@code AccountFeatureStoreSinkListener})가 Redis 쓰기를 마친 직후 발행하는
 * {@link FeatureStoreUpdatedEvent}를 구독해서 매 거래마다 자동으로 판정을 실행한다.
 *
 * <p><b>왜 별도 Kafka 컨슈머 그룹이 아니라 Spring 애플리케이션 이벤트인가</b> — CP1~CP4는 전부
 * "같은 토픽/키를 각자 독립된 컨슈머 그룹으로 구독"하는 패턴을 써왔다(토픽이 계약,
 * docs/WORKTREE_SETUP.md). 처음엔 CP5도 {@code account-feature-updates}를 독립 컨슈머 그룹으로
 * 구독하는 안을 검토했지만, 그러면 CP3의 Redis 쓰기(스냅샷 SET + 최근 시퀀스 RPUSH)와 CP5의
 * 읽기({@code ModelInferenceClient} → Redis LIST 읽기) 사이에 <b>레이스 컨디션</b>이 생긴다 —
 * 서로 다른 컨슈머 그룹은 같은 레코드를 거의 동시에 넘겨받을 뿐 순서를 보장하지 않으므로, CP5가
 * 이번 거래의 RPUSH보다 먼저 모델을 호출하면 "방금 들어온 거래가 반영되지 않은" 시퀀스로 판정하게
 * 된다. Spring의 {@link EventListener}는(별도 TaskExecutor 설정이 없는 한) 발행자와 같은 스레드에서
 * 동기 호출되므로, CP3가 Redis 쓰기를 전부 마친 "이후"에 이벤트를 발행하면 이 레이스가 애초에
 * 생기지 않는다 — 이번엔 정확성이 "단계별 독립 컨슈머 그룹"이라는 기존 패턴의 일관성보다 중요하다고
 * 판단했다(세션 로그 참고).
 *
 * <p>이 트레이드오프의 비용: CP3 파일에 몇 줄(이벤트 발행)이 추가됐다 — 다만 그건 "무엇을
 * 구독하는지 몰라도 되는" 범용 확장 지점({@code ApplicationEventPublisher})이라, CP5의 판정
 * 로직이 아무리 바뀌어도 CP3를 다시 건드릴 필요는 없다.
 *
 * <p><b>실패 격리</b>: 이 리스너의 예외는 여기서 잡고 절대 밖으로 던지지 않는다 — 만약 밖으로
 * 새면 CP3의 Kafka 리스너 스레드까지 예외가 전파되어, 이미 성공한 Redis 쓰기가 있는 레코드가
 * {@code FeatureStoreKafkaConfig}의 {@code DefaultErrorHandler}에 의해 불필요하게 재시도(중복
 * RPUSH 등)될 수 있다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudDecisionEventListener {

    private final FraudDecisionService fraudDecisionService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @EventListener
    public void onFeatureStoreUpdated(FeatureStoreUpdatedEvent event) {
        try {
            RawFeatureStep latestStep = objectMapper.readValue(event.featureJson(), RawFeatureStep.class);
            fraudDecisionService.decide(event.accountId(), latestStep);
        } catch (Exception e) {
            log.warn("CP5 자동 판정 실패, 건너뜀: accountId={}, cause={}", event.accountId(), e.toString());
        }
    }
}
