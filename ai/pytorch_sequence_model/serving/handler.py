"""TorchServe 커스텀 핸들러 — CP4 REST 서빙 계약.

요청 바디(JSON, 계좌 하나당 1건):
{
  "accountId": "acc-123",
  "transactions": [
    {"amountRatio": 1.02, "gapSec": 340.0, "countryChanged": false, "merchantCategory": "GROCERY"},
    ...  // 최근 거래 순서대로(오래된 것 -> 최신), 최대 MAX_SEQ_LEN건 권장(넘으면 이 핸들러가 알아서 최신 것만 사용)
  ]
}

응답(JSON):
{"accountId": "acc-123", "fraudProbability": 0.0731}

주의 — 이 파일은 TorchServe 런타임(`ts` 패키지)에 의존하며, 이 학습 환경(로컬 파이썬)에는
torchserve를 설치하지 않았다(requirements.txt/README "TorchServe 배포" 절 참고). 따라서
이 핸들러는 실제 TorchServe 컨테이너/프로세스 안에서만 로드해서 검증할 수 있고, 이번
세션에서는 정적으로 작성만 하고 실행 검증은 못 했다 — 다음 세션 TODO.
"""

from __future__ import annotations

import json

import torch
from ts.torch_handler.base_handler import BaseHandler

from pytorch_sequence_model.config import MAX_SEQ_LEN, MERCHANT_TO_IDX


class SequenceFraudHandler(BaseHandler):
    def __init__(self):
        super().__init__()
        self._account_ids: list[str] = []

    def preprocess(self, data):
        cont_batch = []
        merchant_batch = []
        length_batch = []
        account_ids = []

        for row in data:
            body = row.get("body") or row.get("data")
            if isinstance(body, (bytes, bytearray)):
                body = json.loads(body.decode("utf-8"))
            elif isinstance(body, str):
                body = json.loads(body)

            account_ids.append(body.get("accountId", "unknown"))
            transactions = body.get("transactions", [])[-MAX_SEQ_LEN:]
            length = len(transactions)

            cont = torch.zeros(MAX_SEQ_LEN, 3, dtype=torch.float32)
            merchant_idx = torch.zeros(MAX_SEQ_LEN, dtype=torch.long)
            for i, tx in enumerate(transactions):
                cont[i, 0] = float(tx.get("amountRatio", 1.0))
                cont[i, 1] = float(tx.get("gapSec", 0.0))
                cont[i, 2] = float(bool(tx.get("countryChanged", False)))
                merchant_idx[i] = MERCHANT_TO_IDX.get(
                    tx.get("merchantCategory", "UNKNOWN"), MERCHANT_TO_IDX["UNKNOWN"]
                )

            cont_batch.append(cont)
            merchant_batch.append(merchant_idx)
            length_batch.append(max(length, 1))  # 빈 이력 방어: 길이 0은 gather 인덱스가 음수가 됨

        self._account_ids = account_ids
        return (
            torch.stack(cont_batch),
            torch.stack(merchant_batch),
            torch.tensor(length_batch, dtype=torch.long),
        )

    def inference(self, data, *args, **kwargs):
        cont, merchant_idx, lengths = data
        with torch.no_grad():
            logits = self.model(cont, merchant_idx, lengths)
            return torch.sigmoid(logits)

    def postprocess(self, data):
        probs = data.tolist()
        return [
            {"accountId": account_id, "fraudProbability": round(prob, 4)}
            for account_id, prob in zip(self._account_ids, probs)
        ]
