package com.fdsv2.transaction;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * k6 부하테스트 진입점.
 *
 * CP1 부하테스트(핫 파티션 재현 시나리오)가 이 엔드포인트로 트래픽을 발생시킨다
 * (docs/PERFORMANCE_MEASUREMENT.md CP1 참고).
 */
@RestController
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionEventProducer producer;

    @PostMapping("/api/transactions")
    public ResponseEntity<Void> submit(@RequestBody TransactionEvent event) {
        producer.publish(event);
        return ResponseEntity.accepted().build();
    }
}
