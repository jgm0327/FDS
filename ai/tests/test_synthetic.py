import unittest

from pytorch_sequence_model.data.synthetic import generate_dataset


class GenerateDatasetTest(unittest.TestCase):
    def test_fraud_ratio_is_approximately_respected(self):
        dataset = generate_dataset(n_accounts=5000, fraud_ratio=0.15, seed=1)

        fraud_rate = sum(seq.label for seq in dataset) / len(dataset)

        self.assertAlmostEqual(fraud_rate, 0.15, delta=0.02)

    def test_same_seed_is_reproducible(self):
        a = generate_dataset(n_accounts=200, fraud_ratio=0.2, seed=7)
        b = generate_dataset(n_accounts=200, fraud_ratio=0.2, seed=7)

        self.assertEqual([seq.label for seq in a], [seq.label for seq in b])
        self.assertEqual(
            [len(seq.steps) for seq in a],
            [len(seq.steps) for seq in b],
        )

    def test_different_seed_changes_data(self):
        a = generate_dataset(n_accounts=200, fraud_ratio=0.2, seed=1)
        b = generate_dataset(n_accounts=200, fraud_ratio=0.2, seed=2)

        self.assertNotEqual([seq.label for seq in a], [seq.label for seq in b])

    def test_every_sequence_has_at_least_one_step(self):
        dataset = generate_dataset(n_accounts=500, fraud_ratio=0.15, seed=3)

        self.assertTrue(all(len(seq.steps) >= 1 for seq in dataset))


if __name__ == "__main__":
    unittest.main()
