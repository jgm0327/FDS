package com.fdsv2.featurestore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@ExtendWith(MockitoExtension.class)
class FeatureQueryControllerTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private FeatureQueryController controller;

    @BeforeEach
    void setUp() {
        controller = new FeatureQueryController(redisTemplate, new FeatureStoreKeyBuilder("feature:account:"));
    }

    @Test
    void 저장된_피처가_있으면_그대로_반환한다() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("feature:account:acc-1")).thenReturn("{\"recentWindowCount\":3}");

        ResponseEntity<String> response = controller.getFeature("acc-1");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo("{\"recentWindowCount\":3}");
    }

    @Test
    void 저장된_피처가_없으면_404를_반환한다() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("feature:account:acc-missing")).thenReturn(null);

        ResponseEntity<String> response = controller.getFeature("acc-missing");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
