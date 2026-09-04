"""AccountSequence 목록 -> 패딩된 텐서 배치로 바꾸는 Dataset/collate.

패딩 전략: 뒤쪽(오른쪽) 0-padding + length를 별도로 전달.
model.py는 length-1 위치를 gather해서 "마지막 유효 타임스텝"의 hidden state를 쓰므로,
padding 값 자체은 어떤 숫자든 결과에 영향을 주지 않는다 (LSTM이 padding 스텝도 계산은
하지만 그 출력은 버려짐 — config.py의 "왜 pack_padded_sequence를 안 쓰는지" 참고).
"""

from __future__ import annotations

import torch
from torch.utils.data import Dataset

from pytorch_sequence_model.config import CONTINUOUS_FEATURE_DIM, MAX_SEQ_LEN
from pytorch_sequence_model.data.schema import AccountSequence


class SequenceFraudDataset(Dataset):
    def __init__(self, sequences: list[AccountSequence]):
        self.sequences = sequences

    def __len__(self) -> int:
        return len(self.sequences)

    def __getitem__(self, idx: int):
        seq = self.sequences[idx]
        # 가장 최근 MAX_SEQ_LEN건만 사용 (그보다 길면 오래된 것부터 버림)
        steps = seq.steps[-MAX_SEQ_LEN:]
        length = len(steps)

        cont = torch.zeros(MAX_SEQ_LEN, CONTINUOUS_FEATURE_DIM, dtype=torch.float32)
        merchant_idx = torch.zeros(MAX_SEQ_LEN, dtype=torch.long)
        for i, step in enumerate(steps):
            cont[i, 0] = step.amount_ratio
            cont[i, 1] = step.gap_sec
            cont[i, 2] = float(step.country_changed)
            merchant_idx[i] = step.merchant_idx()

        label = torch.tensor(float(seq.label), dtype=torch.float32)
        length_t = torch.tensor(length, dtype=torch.long)
        return cont, merchant_idx, length_t, label


def collate_batch(batch):
    cont, merchant_idx, lengths, labels = zip(*batch)
    return (
        torch.stack(cont),
        torch.stack(merchant_idx),
        torch.stack(lengths),
        torch.stack(labels),
    )
