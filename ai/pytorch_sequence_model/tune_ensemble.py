"""CP5 앙상블 가중치/임계값 산정 — 합성 라벨 데이터 기반 그리드서치.

`docs/ARCHITECTURE.md` 5번("판정 및 대응")의 combinedScore = modelWeight*모델확률 +
ruleWeight*규칙점수, 그리고 low/high 임계값(ALLOW/STEP_UP_AUTH/BLOCK 경계)은 지금까지
`EnsembleFraudDecisionService`에 0.7/0.3, 0.3/0.7이라는 값으로 하드코딩되어 있었다 — 근거는
"모델이 시퀀스 전체를 보니 규칙보다 더 믿을 만하다"는 정성적 직관뿐이었다. 이 스크립트는 그
직관을 실제 라벨(합성 데이터, `data/synthetic.py`)로 검증하고, 더 나은 조합이 있는지
그리드서치로 찾는다.

방법론:
1. train.py/evaluate.py와 동일한 seed + split으로 val/test set을 재현한다(재현성 보장).
2. val set에 대해서만 그리드서치(과적합 방지 — evaluate.py가 이미 test set으로 모델 자체를
   평가하므로, 앙상블 파라미터까지 test set에 맞춰 고르면 "test set에 대한 이중 최적화"가 된다).
3. 각 조합의 성능을 "정확도 지표(precision/recall)"가 아니라 **비용 함수**로 비교한다 —
   이 시스템은 이진 판정이 아니라 3단계 액션(허용/추가인증/차단)이라, 단순 F1으로는
   "사기를 추가인증으로 거른 것"과 "완전히 놓친 것"을 구분할 수 없다. 아래
   `CostWeights`의 각 항목 설명 참고.
4. 선택된 조합을 test set에 적용해 최종 수치(비용, 액션 분포, confusion 유사 표)를 보고하고,
   기존 하드코딩 값(0.7/0.3, 0.3/0.7)과 나란히 비교한다.

실행:
    cd ai
    python -m pytorch_sequence_model.train     # 체크포인트가 없으면 먼저 실행
    python -m pytorch_sequence_model.tune_ensemble
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import torch
from torch.utils.data import DataLoader

from pytorch_sequence_model.config import RANDOM_SEED
from pytorch_sequence_model.data.schema import AccountSequence
from pytorch_sequence_model.data.synthetic import generate_dataset
from pytorch_sequence_model.dataset import SequenceFraudDataset, collate_batch
from pytorch_sequence_model.model import SequenceFraudModel
from pytorch_sequence_model.train import ARTIFACTS_DIR, split_dataset

ACTIONS = ("ALLOW", "STEP_UP_AUTH", "BLOCK")

# 지금 EnsembleFraudDecisionService/application.yml에 있는 기존 하드코딩 값 — 비교 기준선.
CURRENT_DEFAULT = {"model_weight": 0.7, "low": 0.3, "high": 0.7}

# RuleEngine의 하드룰(블랙리스트, extreme-amount-ratio-with-country-change)은 화이트리스트와
# 함께 이 앙상블 계산 자체를 건너뛰는 별도 경로(RuleVerdict.override)라 이 스크립트의 범위
# 밖이다 — 여기서 재현하는 건 "애매한 구간"에서만 쓰이는 RuleBasedFallbackScorer 하나뿐이다.
HIGH_AMOUNT_RATIO_THRESHOLD = 5.0


def rule_score(amount_ratio: float, country_changed: bool) -> float:
    """com.fdsv2.modelclient.RuleBasedFallbackScorer#score()의 Python 재구현.

    이 두 구현이 어긋나면 그리드서치 결과가 실제 백엔드 동작과 다른 값을 최적이라고
    잘못 추천하게 된다 — 값 3개(0.1/0.6/0.9)와 분기 조건은 반드시 Java 원본과 동일하게
    유지해야 한다.
    """
    high_amount = amount_ratio >= HIGH_AMOUNT_RATIO_THRESHOLD
    if high_amount and country_changed:
        return 0.9
    if high_amount or country_changed:
        return 0.6
    return 0.1


@dataclass(frozen=True)
class CostWeights:
    """액션 x 실제 라벨 조합별 "비용" 가정치.

    실제 비즈니스 비용(고객 이탈률, 사기 손실액 등) 데이터가 없는 토이 프로젝트라 이
    숫자 자체는 가정이다 — 그리드서치가 이 가정에 따라 결과가 달라지므로, 정확한 값보다
    **상대적 크기 순서**가 중요하다는 점을 명시해 둔다:
      fraud_allow(사기를 완전히 통과) > fraud_step_up(사기를 추가인증으로만 거름, 완전
      차단보다는 위험) > normal_block(정상 거래 차단, 고객 이탈 등 큰 비용) >
      normal_step_up(정상 거래 추가인증, 마찰 비용만) > fraud_block = normal_allow = 0(정답).
    이 가정을 바꾸면 그리드서치 결과도 바뀐다 — 실제 운영 데이터로 각 비용을 다시 추정하는
    것 자체가 다음 개선 과제다(세션 로그 "다음에 이어서 할 일" 참고).
    """

    fraud_allow: float = 20.0
    fraud_step_up: float = 4.0
    fraud_block: float = 0.0
    normal_allow: float = 0.0
    normal_step_up: float = 1.0
    normal_block: float = 6.0

    def as_table(self) -> np.ndarray:
        # 행 = 라벨(0=정상, 1=사기), 열 = 액션(ALLOW, STEP_UP_AUTH, BLOCK) 인덱스 순서.
        return np.array(
            [
                [self.normal_allow, self.normal_step_up, self.normal_block],
                [self.fraud_allow, self.fraud_step_up, self.fraud_block],
            ]
        )


def actions_for(combined: np.ndarray, low: float, high: float) -> np.ndarray:
    """combinedScore 배열 -> 액션 인덱스 배열(0=ALLOW, 1=STEP_UP_AUTH, 2=BLOCK).

    EnsembleFraudDecisionService.toAction()과 동일한 경계 규칙(< low -> ALLOW,
    < high -> STEP_UP_AUTH, 나머지 BLOCK). NaN 완충 처리는 이 스크립트 범위 밖(합성
    데이터에는 NaN이 나올 일이 없다 — 그 방어는 실제 모델 서빙 장애 상황 전용).
    """
    return np.where(combined < low, 0, np.where(combined < high, 1, 2))


def expected_cost(labels: np.ndarray, combined: np.ndarray, low: float, high: float, costs: CostWeights) -> float:
    action = actions_for(combined, low, high)
    table = costs.as_table()
    return float(table[labels, action].mean())


def grid_search(
    labels: np.ndarray,
    model_probs: np.ndarray,
    rule_scores: np.ndarray,
    costs: CostWeights,
    weight_step: float = 0.05,
    threshold_step: float = 0.05,
) -> dict:
    """model_weight/low/high 조합 전체를 훑어 기대 비용이 최소인 조합을 찾는다."""
    weights = np.round(np.arange(0.0, 1.0 + 1e-9, weight_step), 2)
    thresholds = np.round(np.arange(threshold_step, 1.0, threshold_step), 2)

    best = None
    for model_weight in weights:
        combined = model_weight * model_probs + (1 - model_weight) * rule_scores
        for i, low in enumerate(thresholds):
            for high in thresholds[i + 1 :]:
                cost = expected_cost(labels, combined, low, high, costs)
                if best is None or cost < best["cost"]:
                    best = {"cost": cost, "model_weight": float(model_weight), "low": float(low), "high": float(high)}
    return best


def summarize(labels: np.ndarray, combined: np.ndarray, low: float, high: float, costs: CostWeights) -> dict:
    action = actions_for(combined, low, high)
    table = costs.as_table()
    cost = float(table[labels, action].mean())

    counts = {
        f"{'fraud' if lbl else 'normal'}_{ACTIONS[act].lower()}": int(np.sum((labels == lbl) & (action == act)))
        for lbl in (0, 1)
        for act in (0, 1, 2)
    }
    n_fraud = int(np.sum(labels == 1))
    n_normal = int(np.sum(labels == 0))
    return {
        "cost": cost,
        "counts": counts,
        "fraud_leak_rate": counts["fraud_allow"] / n_fraud if n_fraud else float("nan"),
        "fraud_full_block_rate": counts["fraud_block"] / n_fraud if n_fraud else float("nan"),
        "normal_friction_rate": (counts["normal_step_up_auth"] + counts["normal_block"]) / n_normal if n_normal else float("nan"),
    }


def _score_sequences(model: SequenceFraudModel, sequences: list[AccountSequence]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """시퀀스 목록 -> (labels, model_probs, rule_scores) numpy 배열 3종."""
    loader = DataLoader(SequenceFraudDataset(sequences), batch_size=256, shuffle=False, collate_fn=collate_batch)
    all_labels: list[float] = []
    all_probs: list[float] = []
    with torch.no_grad():
        for cont, merchant_idx, lengths, labels in loader:
            probs = model.predict_proba(cont, merchant_idx, lengths)
            all_probs.extend(probs.tolist())
            all_labels.extend(labels.tolist())

    all_rule_scores = [
        rule_score(seq.steps[-1].amount_ratio, seq.steps[-1].country_changed) for seq in sequences
    ]
    return np.array(all_labels, dtype=int), np.array(all_probs), np.array(all_rule_scores)


def tune(
    n_accounts: int = 20_000,
    fraud_ratio: float = 0.15,
    seed: int = RANDOM_SEED,
    checkpoint_path: Path | None = None,
    costs: CostWeights | None = None,
) -> None:
    costs = costs or CostWeights()
    checkpoint_path = checkpoint_path or (ARTIFACTS_DIR / "best_model.pt")
    if not checkpoint_path.exists():
        raise FileNotFoundError(
            f"체크포인트가 없다: {checkpoint_path}. 먼저 `python -m pytorch_sequence_model.train`을 실행해라."
        )

    sequences = generate_dataset(n_accounts=n_accounts, fraud_ratio=fraud_ratio, seed=seed)
    _train_seqs, val_seqs, test_seqs = split_dataset(sequences, train_ratio=0.7, val_ratio=0.15)

    checkpoint = torch.load(checkpoint_path, weights_only=True)
    model = SequenceFraudModel()
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    val_labels, val_probs, val_rules = _score_sequences(model, val_seqs)
    test_labels, test_probs, test_rules = _score_sequences(model, test_seqs)

    print(f"val set: {len(val_seqs)}건 (사기 {int(val_labels.sum())}건, {val_labels.mean():.1%})")
    print(f"test set: {len(test_seqs)}건 (사기 {int(test_labels.sum())}건, {test_labels.mean():.1%})\n")

    print("비용 가정 (CostWeights):")
    print(f"  {costs}\n")

    best = grid_search(val_labels, val_probs, val_rules, costs)
    print("[val set 그리드서치 결과 — 여기서 조합을 고른다]")
    print(f"  model_weight={best['model_weight']:.2f}, rule_weight={1 - best['model_weight']:.2f}, "
          f"low={best['low']:.2f}, high={best['high']:.2f} (기대 비용={best['cost']:.4f})\n")

    def _report(name: str, model_weight: float, low: float, high: float) -> None:
        combined = model_weight * test_probs + (1 - model_weight) * test_rules
        result = summarize(test_labels, combined, low, high, costs)
        print(f"[{name} — test set 기준]")
        print(f"  model_weight={model_weight:.2f}, low={low:.2f}, high={high:.2f}")
        print(f"  기대 비용            : {result['cost']:.4f}")
        print(f"  사기 완전누출율(ALLOW): {result['fraud_leak_rate']:.4f}")
        print(f"  사기 완전차단율(BLOCK): {result['fraud_full_block_rate']:.4f}")
        print(f"  정상거래 마찰율(STEP_UP+BLOCK): {result['normal_friction_rate']:.4f}")
        print(f"  세부 카운트           : {result['counts']}\n")

    _report("기존 하드코딩 값", CURRENT_DEFAULT["model_weight"], CURRENT_DEFAULT["low"], CURRENT_DEFAULT["high"])
    _report("그리드서치로 산정한 값", best["model_weight"], best["low"], best["high"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CP5 앙상블 가중치/임계값 산정")
    parser.add_argument("--n-accounts", type=int, default=20_000)
    parser.add_argument("--fraud-ratio", type=float, default=0.15)
    parser.add_argument("--seed", type=int, default=RANDOM_SEED)
    args = parser.parse_args()

    tune(n_accounts=args.n_accounts, fraud_ratio=args.fraud_ratio, seed=args.seed)
