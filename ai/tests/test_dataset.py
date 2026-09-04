import unittest

from pytorch_sequence_model.config import MAX_SEQ_LEN
from pytorch_sequence_model.data.schema import AccountSequence, TransactionStep
from pytorch_sequence_model.dataset import SequenceFraudDataset


class SequenceFraudDatasetTest(unittest.TestCase):
    def test_short_sequence_is_padded_and_length_recorded(self):
        steps = [
            TransactionStep(amount_ratio=1.0, gap_sec=100.0, country_changed=False, merchant_category="GROCERY"),
            TransactionStep(amount_ratio=2.0, gap_sec=200.0, country_changed=True, merchant_category="FUEL"),
        ]
        seq = AccountSequence(account_id="acc-1", steps=steps, label=1)
        dataset = SequenceFraudDataset([seq])

        cont, merchant_idx, length, label = dataset[0]

        self.assertEqual(cont.shape, (MAX_SEQ_LEN, 3))
        self.assertEqual(merchant_idx.shape, (MAX_SEQ_LEN,))
        self.assertEqual(length.item(), 2)
        self.assertEqual(label.item(), 1.0)
        # 실제 2건은 정확히 인코딩됨
        self.assertAlmostEqual(cont[0, 0].item(), 1.0)
        self.assertAlmostEqual(cont[1, 0].item(), 2.0)
        self.assertEqual(cont[1, 2].item(), 1.0)
        # 나머지는 0-padding
        self.assertTrue((cont[2:] == 0).all())
        self.assertTrue((merchant_idx[2:] == 0).all())

    def test_long_sequence_is_truncated_to_most_recent_steps(self):
        steps = [
            TransactionStep(amount_ratio=float(i), gap_sec=1.0, country_changed=False)
            for i in range(MAX_SEQ_LEN + 5)
        ]
        seq = AccountSequence(account_id="acc-2", steps=steps, label=0)
        dataset = SequenceFraudDataset([seq])

        cont, _merchant_idx, length, _label = dataset[0]

        self.assertEqual(length.item(), MAX_SEQ_LEN)
        # 가장 오래된 5건(0~4)은 버려지고, 가장 최근 MAX_SEQ_LEN건만 남아야 함
        self.assertAlmostEqual(cont[0, 0].item(), 5.0)
        self.assertAlmostEqual(cont[-1, 0].item(), float(MAX_SEQ_LEN + 4))


if __name__ == "__main__":
    unittest.main()
