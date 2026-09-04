"""CP4 시퀀스 모델 — LSTM 기반 이진 분류기.

LSTM vs Transformer 선택 근거 (ARCHITECTURE.md는 "LSTM 또는 소규모 Transformer 인코더"로
둘 다 열어둠):
- 시퀀스 길이가 짧다(MAX_SEQ_LEN=30) — Transformer의 병렬화 이점이 크지 않음.
- CP4 성능 측정 목표가 "추론 latency p50/p95/p99"이자 Circuit Breaker 타임아웃 산정
  근거인데, Self-Attention은 같은 hidden size 기준으로 LSTM보다 파라미터/연산이 많고
  패딩 마스크 처리도 추가로 필요해 서빙 latency에 불리하다.
- LSTM은 pack 없이 순차 처리해도(아래 참고) 짧은 시퀀스에서는 연산량이 작고,
  TorchScript trace가 Attention/마스킹보다 단순해 CP4의 "REST 먼저, 안정적으로" 원칙과
  맞는다.
- 트레이드오프: 시퀀스가 훨씬 길어지거나(수백 스텝) 스텝 간 장거리 의존성이 중요해지면
  Transformer가 유리해질 수 있다 — 다음 개선 후보로 ai/README.md에 남겨둠.

pack_padded_sequence를 쓰지 않는 이유: 정확도상 이점(패딩 스텝 연산 스킵)은 있지만,
가변 길이 제어 흐름이 TorchScript trace 기반 export를 복잡하게 만든다(스크립팅으로
우회 가능하나 커스텀 핸들러 유지보수 비용이 커짐). 대신 패딩 스텝까지 전부 LSTM에
통과시키되, gather로 "실제 마지막 유효 타임스텝"의 hidden state만 뽑아 쓴다 — 결과는
동일하고(패딩 스텝의 출력은 버려짐) trace가 단순해진다. MAX_SEQ_LEN=30 규모에서는
낭비되는 연산이 무시할 만하다고 판단.
"""

from __future__ import annotations

import torch
import torch.nn as nn

from pytorch_sequence_model.config import (
    CONTINUOUS_FEATURE_DIM,
    DROPOUT,
    LSTM_HIDDEN_DIM,
    LSTM_NUM_LAYERS,
    MERCHANT_CATEGORIES,
    MERCHANT_EMBEDDING_DIM,
)


class SequenceFraudModel(nn.Module):
    def __init__(
        self,
        num_merchant_categories: int = len(MERCHANT_CATEGORIES),
        continuous_dim: int = CONTINUOUS_FEATURE_DIM,
        merchant_embedding_dim: int = MERCHANT_EMBEDDING_DIM,
        hidden_dim: int = LSTM_HIDDEN_DIM,
        num_layers: int = LSTM_NUM_LAYERS,
        dropout: float = DROPOUT,
    ):
        super().__init__()
        self.merchant_embedding = nn.Embedding(num_merchant_categories, merchant_embedding_dim)
        lstm_input_dim = continuous_dim + merchant_embedding_dim
        self.lstm = nn.LSTM(
            input_size=lstm_input_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.head = nn.Sequential(
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Linear(hidden_dim // 2, 1),
        )

    def forward(
        self,
        cont_raw: torch.Tensor,  # [B, T, 3] = (amount_ratio, gap_sec, country_changed) — 원시값
        merchant_idx: torch.Tensor,  # [B, T] long
        lengths: torch.Tensor,  # [B] long, 각 시퀀스의 실제 길이(1..MAX_SEQ_LEN)
    ) -> torch.Tensor:
        # amount_ratio/gap_sec는 값의 스케일 편차가 커서 log1p로 안정화.
        # country_changed(마지막 컬럼)는 이미 0/1이라 그대로 둔다.
        amount_ratio = torch.log1p(cont_raw[..., 0:1])
        gap_sec = torch.log1p(cont_raw[..., 1:2])
        country_changed = cont_raw[..., 2:3]
        cont = torch.cat([amount_ratio, gap_sec, country_changed], dim=-1)

        merchant_emb = self.merchant_embedding(merchant_idx)  # [B, T, E]
        lstm_input = torch.cat([cont, merchant_emb], dim=-1)  # [B, T, 3+E]

        lstm_out, _ = self.lstm(lstm_input)  # [B, T, H]

        # 각 시퀀스의 "마지막 유효 타임스텝"(length-1) hidden state만 gather.
        batch_size, _, hidden_dim = lstm_out.shape
        last_idx = (lengths - 1).clamp(min=0).view(batch_size, 1, 1).expand(batch_size, 1, hidden_dim)
        last_hidden = lstm_out.gather(1, last_idx).squeeze(1)  # [B, H]

        logits = self.head(last_hidden).squeeze(-1)  # [B]
        return logits

    def predict_proba(self, *args, **kwargs) -> torch.Tensor:
        return torch.sigmoid(self.forward(*args, **kwargs))
