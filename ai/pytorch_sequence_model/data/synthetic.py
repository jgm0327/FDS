"""합성(가짜) 계좌 거래 시퀀스 생성기.

CLAUDE.md/WORKTREE_SETUP.md 원칙: ai/pytorch-sequence-model 브랜치는 실제 CP2/CP3
데이터 없이도 "가짜 데이터로 병렬 진행" 가능해야 한다. 이 모듈이 그 가짜 데이터를 만든다.

설계 의도:
- 정상 시퀀스는 금액배율 1 근처, 적당한 거래 간격, 국가변경 드묾, 특정 이상 패턴 없음.
- 사기 시퀀스는 "정상처럼 보이는 앞부분(burn-in)" 뒤에 마지막 1~3건에서 이상 패턴을 주입.
  이상 패턴 종류:
    1) amount_spike: 평소 대비 금액이 갑자기 3~8배 (완전히 분리되지 않도록 정상 분포와
       겹치는 구간을 일부 남김 — 아래 "왜 완벽히 분리하지 않았나" 참고)
    2) burst_frequency: 매우 짧은 간격(수 초~수십 초)으로 연속 거래
    3) country_change_plus_spike: 국가변경과 동시에 금액도 소폭 상승 (단일 신호로는
       약하지만 두 신호가 겹치면 의심스러운 복합 패턴)
- 왜 완벽히 분리하지 않았나: 이상 패턴의 강도를 정상 분포와 약간 겹치게 설계해서,
  "고정 임계값 규칙 하나"로는 못 잡고 시퀀스 전체 맥락(평소 패턴 대비 편차, 여러 신호의
  조합)을 봐야 잡히게 만들었다. 이래야 evaluate.py의 "단건 기준 베이스라인 vs 시퀀스
  모델" 비교가 의미를 가진다 — 단건 베이스라인이 이미 100% 잡아내면 시퀀스 모델의
  존재 이유를 증명할 수 없다.
"""

from __future__ import annotations

import random

from pytorch_sequence_model.config import MERCHANT_CATEGORIES
from pytorch_sequence_model.data.schema import AccountSequence, TransactionStep

_NORMAL_MERCHANTS = [m for m in MERCHANT_CATEGORIES if m not in ("CASH_ADVANCE", "UNKNOWN")]


def _normal_step(rng: random.Random) -> TransactionStep:
    amount_ratio = max(0.05, rng.lognormvariate(0.0, 0.3))
    gap_sec = max(1.0, rng.lognormvariate(8.0, 1.0))  # 평균 수십분~수시간대
    country_changed = rng.random() < 0.03
    merchant = rng.choice(_NORMAL_MERCHANTS)
    return TransactionStep(amount_ratio, gap_sec, country_changed, merchant)


def _generate_normal_sequence(rng: random.Random, account_id: str) -> AccountSequence:
    seq_len = rng.randint(3, 30)
    steps = [_normal_step(rng) for _ in range(seq_len)]
    return AccountSequence(account_id=account_id, steps=steps, label=0)


def _inject_amount_spike(rng: random.Random, step: TransactionStep) -> TransactionStep:
    spike_ratio = rng.uniform(3.0, 8.0)
    return TransactionStep(
        amount_ratio=step.amount_ratio * spike_ratio,
        gap_sec=step.gap_sec,
        country_changed=step.country_changed,
        merchant_category=step.merchant_category,
    )


def _inject_burst_frequency(rng: random.Random, step: TransactionStep) -> TransactionStep:
    return TransactionStep(
        amount_ratio=step.amount_ratio,
        gap_sec=rng.uniform(1.0, 30.0),
        country_changed=step.country_changed,
        merchant_category=step.merchant_category,
    )


def _inject_country_change_plus_spike(rng: random.Random, step: TransactionStep) -> TransactionStep:
    return TransactionStep(
        amount_ratio=step.amount_ratio * rng.uniform(1.5, 3.0),
        gap_sec=step.gap_sec,
        country_changed=True,
        merchant_category=step.merchant_category,
    )


_ANOMALY_INJECTORS = [
    _inject_amount_spike,
    _inject_burst_frequency,
    _inject_country_change_plus_spike,
]


def _generate_fraud_sequence(rng: random.Random, account_id: str) -> AccountSequence:
    burn_in_len = rng.randint(2, 27)
    steps = [_normal_step(rng) for _ in range(burn_in_len)]

    anomaly_tail_len = rng.randint(1, 3)
    injector = rng.choice(_ANOMALY_INJECTORS)
    for _ in range(anomaly_tail_len):
        steps.append(injector(rng, _normal_step(rng)))

    return AccountSequence(account_id=account_id, steps=steps, label=1)


def generate_dataset(
    n_accounts: int,
    fraud_ratio: float = 0.15,
    seed: int = 42,
) -> list[AccountSequence]:
    """길이 n_accounts의 합성 계좌 시퀀스 목록을 만든다. 라벨 비율은 fraud_ratio 근사치."""
    rng = random.Random(seed)
    dataset: list[AccountSequence] = []
    for i in range(n_accounts):
        account_id = f"synthetic-acc-{i}"
        if rng.random() < fraud_ratio:
            dataset.append(_generate_fraud_sequence(rng, account_id))
        else:
            dataset.append(_generate_normal_sequence(rng, account_id))
    rng.shuffle(dataset)
    return dataset
