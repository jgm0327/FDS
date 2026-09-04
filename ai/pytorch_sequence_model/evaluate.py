"""CP4 시퀀스 모델 평가 + "단건 기준 베이스라인" 비교.

docs/PERFORMANCE_MEASUREMENT.md "개선 전/후 비교 방법"이 요구하는 두 관점 중
"정확도/탐지력 관점"(시퀀스 기반이 단건 대비 어떤 패턴을 추가로 잡아내는지)을
이 스크립트가 담당한다. 성능 비용 관점(latency)은 실제 TorchServe 배포 후
Prometheus/k6로 별도 측정한다(ai/README.md TODO 참고).

베이스라인 = 시퀀스의 "마지막 거래 1건"만 보는 로지스틱 회귀. 기존 FDS(단건 판단)를
근사한다. 같은 학습/평가 split을 쓰므로 AUC 차이가 곧 "시퀀스 맥락을 보는 것의 순수한
효과"에 가깝다.

실행:
    cd ai
    python -m pytorch_sequence_model.evaluate
"""

from __future__ import annotations

from pathlib import Path

import torch
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support, roc_auc_score
from torch.utils.data import DataLoader

from pytorch_sequence_model.config import RANDOM_SEED
from pytorch_sequence_model.data.synthetic import generate_dataset
from pytorch_sequence_model.dataset import SequenceFraudDataset, collate_batch
from pytorch_sequence_model.model import SequenceFraudModel
from pytorch_sequence_model.train import ARTIFACTS_DIR, split_dataset


def _last_step_features(sequences: list) -> tuple[list[list[float]], list[int]]:
    """베이스라인용: 각 시퀀스의 마지막 거래 1건만 뽑아 (amount_ratio, gap_sec, country_changed) 반환."""
    X, y = [], []
    for seq in sequences:
        last = seq.steps[-1]
        X.append([last.amount_ratio, last.gap_sec, float(last.country_changed)])
        y.append(seq.label)
    return X, y


def evaluate(
    n_accounts: int = 20_000,
    fraud_ratio: float = 0.15,
    seed: int = RANDOM_SEED,
    checkpoint_path: Path | None = None,
    threshold: float = 0.5,
) -> None:
    checkpoint_path = checkpoint_path or (ARTIFACTS_DIR / "best_model.pt")
    if not checkpoint_path.exists():
        raise FileNotFoundError(
            f"체크포인트가 없다: {checkpoint_path}. 먼저 `python -m pytorch_sequence_model.train`을 실행해라."
        )

    # 학습 때와 동일한 seed + 동일한 split 함수 -> 동일한 test set 재현
    sequences = generate_dataset(n_accounts=n_accounts, fraud_ratio=fraud_ratio, seed=seed)
    train_seqs, _val_seqs, test_seqs = split_dataset(sequences, train_ratio=0.7, val_ratio=0.15)

    # --- 1) 시퀀스 모델(LSTM) 평가 ---
    checkpoint = torch.load(checkpoint_path, weights_only=True)
    model = SequenceFraudModel()
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    test_loader = DataLoader(
        SequenceFraudDataset(test_seqs), batch_size=256, shuffle=False, collate_fn=collate_batch
    )
    all_labels, all_probs = [], []
    with torch.no_grad():
        for cont, merchant_idx, lengths, labels in test_loader:
            probs = model.predict_proba(cont, merchant_idx, lengths)
            all_probs.extend(probs.tolist())
            all_labels.extend(labels.tolist())

    seq_auc = roc_auc_score(all_labels, all_probs)
    seq_preds = [1 if p >= threshold else 0 for p in all_probs]
    seq_precision, seq_recall, seq_f1, _ = precision_recall_fscore_support(
        all_labels, seq_preds, average="binary", zero_division=0
    )
    seq_cm = confusion_matrix(all_labels, seq_preds)

    # --- 2) 베이스라인(마지막 거래 1건 기준 로지스틱 회귀) 평가 ---
    X_train, y_train = _last_step_features(train_seqs)
    X_test, y_test = _last_step_features(test_seqs)
    baseline = LogisticRegression(class_weight="balanced", max_iter=1000)
    baseline.fit(X_train, y_train)
    baseline_probs = baseline.predict_proba(X_test)[:, 1]
    baseline_auc = roc_auc_score(y_test, baseline_probs)
    baseline_preds = [1 if p >= threshold else 0 for p in baseline_probs]
    baseline_precision, baseline_recall, baseline_f1, _ = precision_recall_fscore_support(
        y_test, baseline_preds, average="binary", zero_division=0
    )
    baseline_cm = confusion_matrix(y_test, baseline_preds)

    print(f"test set: {len(test_seqs)}건 (사기 {sum(y_test)}건, {sum(y_test) / len(y_test):.1%})\n")

    print("[시퀀스 모델 (LSTM, 최근 최대 30건 컨텍스트)]")
    print(f"  AUC       : {seq_auc:.4f}")
    print(f"  Precision : {seq_precision:.4f}")
    print(f"  Recall    : {seq_recall:.4f}")
    print(f"  F1        : {seq_f1:.4f}")
    print(f"  Confusion matrix [[TN FP][FN TP]]:\n{seq_cm}\n")

    print("[베이스라인 (마지막 거래 1건, 로지스틱 회귀 - 기존 FDS 단건 판단 근사)]")
    print(f"  AUC       : {baseline_auc:.4f}")
    print(f"  Precision : {baseline_precision:.4f}")
    print(f"  Recall    : {baseline_recall:.4f}")
    print(f"  F1        : {baseline_f1:.4f}")
    print(f"  Confusion matrix [[TN FP][FN TP]]:\n{baseline_cm}\n")

    print(f"AUC 차이 (시퀀스 - 베이스라인): {seq_auc - baseline_auc:+.4f}")


if __name__ == "__main__":
    evaluate()
