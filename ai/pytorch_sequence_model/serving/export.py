"""학습된 체크포인트 -> TorchScript(.pt)로 export.

TorchServe는 자바(TorchServe 프로세스)에서 파이썬 클래스 정의 없이 모델을 로드해야 하므로
state_dict가 아니라 TorchScript(trace) 결과물을 넘긴다.

trace를 쓰는 이유(script 대신): model.py의 forward는 텐서 연산만으로 구성되어 있고
(Python 제어 흐름에 따라 그래프가 달라지는 분기가 없음 — config.py/model.py 주석 참고),
고정 shape(batch, MAX_SEQ_LEN, ...) 예시 입력 하나로 trace하면 충분하다. script는
TorchScript 서브셋 문법 제약이 많아 유지보수 비용이 더 크다.

실행:
    cd ai
    python -m pytorch_sequence_model.serving.export

출력: ai/artifacts/traced_model.pt

그 다음 TorchServe로 배포하려면(torchserve/torch-model-archiver 설치 필요 — README 참고):
    torch-model-archiver \
      --model-name fds-sequence-model \
      --version 1.0 \
      --serialized-file ai/artifacts/traced_model.pt \
      --handler ai/pytorch_sequence_model/serving/handler.py \
      --export-path ai/model_store

    torchserve --start --model-store ai/model_store --models fds-sequence-model=fds-sequence-model.mar
"""

from __future__ import annotations

from pathlib import Path

import torch

from pytorch_sequence_model.config import MAX_SEQ_LEN
from pytorch_sequence_model.model import SequenceFraudModel
from pytorch_sequence_model.train import ARTIFACTS_DIR


def export(checkpoint_path: Path | None = None, output_path: Path | None = None) -> Path:
    checkpoint_path = checkpoint_path or (ARTIFACTS_DIR / "best_model.pt")
    output_path = output_path or (ARTIFACTS_DIR / "traced_model.pt")

    if not checkpoint_path.exists():
        raise FileNotFoundError(
            f"체크포인트가 없다: {checkpoint_path}. 먼저 `python -m pytorch_sequence_model.train`을 실행해라."
        )

    checkpoint = torch.load(checkpoint_path, weights_only=True)
    model = SequenceFraudModel()
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    example_cont = torch.zeros(1, MAX_SEQ_LEN, 3, dtype=torch.float32)
    example_merchant_idx = torch.zeros(1, MAX_SEQ_LEN, dtype=torch.long)
    example_lengths = torch.tensor([MAX_SEQ_LEN], dtype=torch.long)

    traced = torch.jit.trace(model, (example_cont, example_merchant_idx, example_lengths))

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    traced.save(str(output_path))
    print(f"traced model saved to {output_path}")
    return output_path


if __name__ == "__main__":
    export()
