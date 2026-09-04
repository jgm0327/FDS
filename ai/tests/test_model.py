import unittest

import torch

from pytorch_sequence_model.config import MAX_SEQ_LEN
from pytorch_sequence_model.model import SequenceFraudModel


class SequenceFraudModelTest(unittest.TestCase):
    def setUp(self):
        torch.manual_seed(0)
        self.model = SequenceFraudModel()
        self.model.eval()

    def test_output_shape(self):
        cont = torch.rand(4, MAX_SEQ_LEN, 3)
        merchant_idx = torch.randint(0, 5, (4, MAX_SEQ_LEN))
        lengths = torch.tensor([30, 10, 1, 30])

        logits = self.model(cont, merchant_idx, lengths)

        self.assertEqual(logits.shape, (4,))

    def test_padding_values_after_length_do_not_affect_output(self):
        """gather가 length-1까지만 보므로, 그 뒤 padding에 어떤 값을 넣든 출력이 같아야 한다."""
        length = 5
        cont_a = torch.rand(1, MAX_SEQ_LEN, 3)
        cont_a[:, length:, :] = 0.0
        cont_b = cont_a.clone()
        cont_b[:, length:, :] = torch.rand(1, MAX_SEQ_LEN - length, 3) * 999.0  # 쓰레기 값

        merchant_idx = torch.zeros(1, MAX_SEQ_LEN, dtype=torch.long)
        lengths = torch.tensor([length])

        with torch.no_grad():
            out_a = self.model(cont_a, merchant_idx, lengths)
            out_b = self.model(cont_b, merchant_idx, lengths)

        # LSTM은 padding 스텝도 순차 계산하므로 hidden state 자체는 padding 이후 값에 영향받지만,
        # gather는 length-1(=4) 위치만 뽑으므로 그 이후(5~29) 값이 뭐든 결과는 같아야 한다.
        self.assertTrue(torch.allclose(out_a, out_b, atol=1e-6))

    def test_predict_proba_is_in_unit_interval(self):
        cont = torch.rand(2, MAX_SEQ_LEN, 3)
        merchant_idx = torch.randint(0, 5, (2, MAX_SEQ_LEN))
        lengths = torch.tensor([30, 15])

        probs = self.model.predict_proba(cont, merchant_idx, lengths)

        self.assertTrue(torch.all(probs >= 0.0))
        self.assertTrue(torch.all(probs <= 1.0))


if __name__ == "__main__":
    unittest.main()
