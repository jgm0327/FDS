"""CP4 모델의 입력 계약(스키마).

거래 1건(TransactionStep)의 필드는 CP2가 만드는 AccountFeatureVector와 같은 의미론을
공유한다 — 다만 CP2/CP3는 "최신 값 1개(집계 결과)"만 Redis에 남기는 반면, 이 모델은
"계좌의 최근 N건 각각"을 스텝으로 받아 시퀀스로 본다. 즉 CP4가 요구하는 스텝별 원본
이력은 현재 CP2/CP3 파이프라인이 그대로 제공하지 않는다 — 이 간극은 ai/README.md의
"백엔드 연동 시 남는 숙제" 절에 기록해 두었다 (backend/model-client 브랜치 담당).

- amount_ratio: 이 거래 금액 / 그 시점까지의 평소 금액 (CP2 amountRatio와 동일 정의)
- gap_sec: 직전 거래 이후 경과 시간(초). 계좌의 첫 거래는 0.0으로 둔다
  (CP2는 null이지만, 텐서 인코딩에서는 "직전 거래 없음"을 0으로 표현하고
  대신 이 스텝이 시퀀스의 첫 스텝인지는 country_changed=False, gap_sec=0 조합 및
  시퀀스 길이(length)로 모델이 간접적으로 알 수 있음 — 별도 마스크 피처는 MVP 범위 밖).
- country_changed: 직전 거래와 국가가 다르면 1.0, 아니면 0.0
- merchant_category: config.MERCHANT_CATEGORIES 중 하나 (모르면 "UNKNOWN")
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pytorch_sequence_model.config import MERCHANT_TO_IDX


@dataclass(frozen=True)
class TransactionStep:
    amount_ratio: float
    gap_sec: float
    country_changed: bool
    merchant_category: str = "UNKNOWN"

    def merchant_idx(self) -> int:
        return MERCHANT_TO_IDX.get(self.merchant_category, MERCHANT_TO_IDX["UNKNOWN"])


@dataclass(frozen=True)
class AccountSequence:
    """계좌 하나의 최근 거래 시퀀스 + 라벨(학습/평가용).

    실서빙 입력은 라벨이 없다 — TorchServe 핸들러는 steps만 받는다.
    """

    account_id: str
    steps: list[TransactionStep] = field(default_factory=list)
    label: int = 0  # 0=정상, 1=사기(추후 확정 라벨 기준)

    def __post_init__(self) -> None:
        if not self.steps:
            raise ValueError("AccountSequence must have at least one step")
        if self.label not in (0, 1):
            raise ValueError(f"label must be 0 or 1, got {self.label}")
