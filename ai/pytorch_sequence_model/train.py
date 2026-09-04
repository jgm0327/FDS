"""CP4 시퀀스 모델 학습 스크립트.

실행:
    cd ai
    python -m pytorch_sequence_model.train

합성 데이터로 학습하므로 별도 데이터 파일이 필요 없다 — RANDOM_SEED로 재현 가능.
결과물(체크포인트)은 ai/artifacts/에 저장되며 .gitignore에 의해 커밋되지 않는다
(재현 가능한 스크립트만 커밋 대상 — ai/README.md "왜 학습 산출물을 커밋하지 않는지" 참고).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import torch
from sklearn.metrics import roc_auc_score
from torch.utils.data import DataLoader

from pytorch_sequence_model.config import RANDOM_SEED
from pytorch_sequence_model.data.synthetic import generate_dataset
from pytorch_sequence_model.dataset import SequenceFraudDataset, collate_batch
from pytorch_sequence_model.model import SequenceFraudModel

ARTIFACTS_DIR = Path(__file__).resolve().parent.parent / "artifacts"


def split_dataset(sequences: list, train_ratio: float, val_ratio: float):
    """train/val/test로 분할. evaluate.py도 같은 seed + 같은 함수로 test split을 재현한다."""
    n = len(sequences)
    n_train = int(n * train_ratio)
    n_val = int(n * val_ratio)
    return (
        sequences[:n_train],
        sequences[n_train : n_train + n_val],
        sequences[n_train + n_val :],
    )


@torch.no_grad()
def _evaluate(model: SequenceFraudModel, loader: DataLoader) -> tuple[float, float]:
    model.eval()
    all_labels, all_probs = [], []
    total_loss = 0.0
    n_batches = 0
    loss_fn = torch.nn.BCEWithLogitsLoss()
    for cont, merchant_idx, lengths, labels in loader:
        logits = model(cont, merchant_idx, lengths)
        loss = loss_fn(logits, labels)
        total_loss += loss.item()
        n_batches += 1
        all_probs.extend(torch.sigmoid(logits).tolist())
        all_labels.extend(labels.tolist())
    auc = roc_auc_score(all_labels, all_probs) if len(set(all_labels)) > 1 else float("nan")
    return total_loss / max(n_batches, 1), auc


def train(
    n_accounts: int = 20_000,
    fraud_ratio: float = 0.15,
    epochs: int = 15,
    batch_size: int = 64,
    lr: float = 1e-3,
    seed: int = RANDOM_SEED,
) -> Path:
    torch.manual_seed(seed)

    sequences = generate_dataset(n_accounts=n_accounts, fraud_ratio=fraud_ratio, seed=seed)
    train_seqs, val_seqs, test_seqs = split_dataset(sequences, train_ratio=0.7, val_ratio=0.15)

    train_loader = DataLoader(
        SequenceFraudDataset(train_seqs), batch_size=batch_size, shuffle=True, collate_fn=collate_batch
    )
    val_loader = DataLoader(
        SequenceFraudDataset(val_seqs), batch_size=batch_size, shuffle=False, collate_fn=collate_batch
    )

    model = SequenceFraudModel()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # 라벨 불균형(fraud_ratio ~15%) 보정: pos_weight = 정상건수/사기건수
    n_pos = sum(seq.label for seq in train_seqs)
    n_neg = len(train_seqs) - n_pos
    pos_weight = torch.tensor(n_neg / max(n_pos, 1), dtype=torch.float32)
    loss_fn = torch.nn.BCEWithLogitsLoss(pos_weight=pos_weight)

    best_val_auc = -1.0
    best_state = None

    for epoch in range(1, epochs + 1):
        model.train()
        train_loss = 0.0
        n_batches = 0
        for cont, merchant_idx, lengths, labels in train_loader:
            optimizer.zero_grad()
            logits = model(cont, merchant_idx, lengths)
            loss = loss_fn(logits, labels)
            loss.backward()
            optimizer.step()
            train_loss += loss.item()
            n_batches += 1

        val_loss, val_auc = _evaluate(model, val_loader)
        print(
            f"epoch {epoch:2d} | train_loss {train_loss / max(n_batches, 1):.4f} "
            f"| val_loss {val_loss:.4f} | val_auc {val_auc:.4f}"
        )

        if val_auc > best_val_auc:
            best_val_auc = val_auc
            best_state = {k: v.clone() for k, v in model.state_dict().items()}

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_path = ARTIFACTS_DIR / "best_model.pt"
    torch.save(
        {
            "model_state_dict": best_state,
            "val_auc": best_val_auc,
            "seed": seed,
        },
        checkpoint_path,
    )
    print(f"\nbest val_auc={best_val_auc:.4f} -> saved to {checkpoint_path}")

    # 테스트셋도 함께 저장해서 evaluate.py가 학습 때와 동일한 test split을 재현할 수 있게
    # seed만 넘겨주면 된다 (generate_dataset + 같은 split 비율은 결정적이므로 파일 저장 불필요).
    return checkpoint_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CP4 시퀀스 모델 학습")
    parser.add_argument("--n-accounts", type=int, default=20_000)
    parser.add_argument("--fraud-ratio", type=float, default=0.15)
    parser.add_argument("--epochs", type=int, default=15)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--seed", type=int, default=RANDOM_SEED)
    args = parser.parse_args()

    train(
        n_accounts=args.n_accounts,
        fraud_ratio=args.fraud_ratio,
        epochs=args.epochs,
        batch_size=args.batch_size,
        lr=args.lr,
        seed=args.seed,
    )
