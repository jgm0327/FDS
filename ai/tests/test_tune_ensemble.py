import unittest

import numpy as np

from pytorch_sequence_model.tune_ensemble import (
    CostWeights,
    actions_for,
    expected_cost,
    grid_search,
    rule_score,
)


class RuleScoreTest(unittest.TestCase):
    """com.fdsv2.modelclient.RuleBasedFallbackScorerTest와 1:1 대응시켜야 한다 — 두 구현이
    어긋나면 그리드서치가 실제 백엔드와 다른 값을 최적이라고 잘못 추천하게 된다."""

    def test_plain_transaction_gets_low_score(self):
        self.assertEqual(rule_score(amount_ratio=1.1, country_changed=False), 0.1)

    def test_high_amount_only_gets_mid_score(self):
        self.assertEqual(rule_score(amount_ratio=6.0, country_changed=False), 0.6)

    def test_country_change_only_gets_mid_score(self):
        self.assertEqual(rule_score(amount_ratio=1.0, country_changed=True), 0.6)

    def test_high_amount_and_country_change_gets_high_score(self):
        self.assertEqual(rule_score(amount_ratio=7.0, country_changed=True), 0.9)

    def test_boundary_5_0_counts_as_high_amount(self):
        self.assertEqual(rule_score(amount_ratio=5.0, country_changed=False), 0.6)


class ActionsForTest(unittest.TestCase):
    def test_below_low_is_allow(self):
        actions = actions_for(np.array([0.1]), low=0.3, high=0.7)
        self.assertEqual(actions[0], 0)

    def test_between_low_and_high_is_step_up_auth(self):
        actions = actions_for(np.array([0.5]), low=0.3, high=0.7)
        self.assertEqual(actions[0], 1)

    def test_at_or_above_high_is_block(self):
        actions = actions_for(np.array([0.9]), low=0.3, high=0.7)
        self.assertEqual(actions[0], 2)

    def test_boundaries_fall_into_the_next_bucket(self):
        # EnsembleFraudDecisionService.toAction()과 동일: "< low"/"< high" (엄격한 미만) 비교.
        actions = actions_for(np.array([0.3, 0.7]), low=0.3, high=0.7)
        self.assertEqual(actions[0], 1)  # 0.3은 ALLOW가 아니라 STEP_UP_AUTH
        self.assertEqual(actions[1], 2)  # 0.7은 STEP_UP_AUTH가 아니라 BLOCK


class ExpectedCostTest(unittest.TestCase):
    def test_perfect_separation_costs_nothing(self):
        labels = np.array([1, 1, 0, 0])
        combined = np.array([0.9, 0.9, 0.1, 0.1])
        cost = expected_cost(labels, combined, low=0.3, high=0.7, costs=CostWeights())
        self.assertEqual(cost, 0.0)

    def test_allowing_all_fraud_costs_exactly_fraud_allow(self):
        labels = np.array([1, 1])
        combined = np.array([0.1, 0.1])  # low 미만 -> ALLOW
        costs = CostWeights(fraud_allow=20.0)
        cost = expected_cost(labels, combined, low=0.3, high=0.7, costs=costs)
        self.assertEqual(cost, 20.0)


class GridSearchTest(unittest.TestCase):
    def test_relying_on_model_is_necessary_when_rule_score_is_uninformative(self):
        # 사기는 모델확률 1.0, 정상은 0.0으로 완벽히 분리되지만, 규칙점수는 라벨과 무관하게
        # 전부 0.5(변별력 없음). model_weight=0(규칙만)이면 combinedScore가 라벨과 무관하게
        # 전부 같은 값이 되어 어떤 임계값을 써도 분리가 불가능하다는 것을 먼저 확인하고,
        # 그리드서치가 실제로 model_weight>0인 조합(비용 0)을 찾아내는지 검증한다. 정확히 어떤
        # model_weight 값이 최소인지는 threshold_step 그리드 해상도에 좌우되므로 단언하지 않는다.
        labels = np.array([1, 1, 1, 0, 0, 0])
        model_probs = np.array([1.0, 1.0, 1.0, 0.0, 0.0, 0.0])
        rule_scores = np.array([0.5, 0.5, 0.5, 0.5, 0.5, 0.5])

        rule_only_combined = 0.0 * model_probs + 1.0 * rule_scores
        self.assertGreater(
            expected_cost(labels, rule_only_combined, low=0.3, high=0.7, costs=CostWeights()), 0.0
        )

        best = grid_search(labels, model_probs, rule_scores, CostWeights())

        self.assertGreater(best["model_weight"], 0.0)
        self.assertEqual(best["cost"], 0.0)

    def test_relying_on_rule_is_necessary_when_model_score_is_uninformative(self):
        # 위 테스트의 대칭 케이스 — 이번엔 규칙점수가 완벽히 분리하고 모델확률이 변별력 없음.
        labels = np.array([1, 1, 1, 0, 0, 0])
        model_probs = np.array([0.5, 0.5, 0.5, 0.5, 0.5, 0.5])
        rule_scores = np.array([0.9, 0.9, 0.9, 0.1, 0.1, 0.1])

        model_only_combined = 1.0 * model_probs + 0.0 * rule_scores
        self.assertGreater(
            expected_cost(labels, model_only_combined, low=0.3, high=0.7, costs=CostWeights()), 0.0
        )

        best = grid_search(labels, model_probs, rule_scores, CostWeights())

        self.assertLess(best["model_weight"], 1.0)
        self.assertEqual(best["cost"], 0.0)


if __name__ == "__main__":
    unittest.main()
