"""CP4 시퀀스 모델의 하이퍼파라미터/피처 스키마 상수.

이 파일의 값들이 곧 학습(train.py)과 서빙(serving/handler.py)이 공유하는 계약이다.
값을 바꾸면 학습된 체크포인트와 export된 TorchScript가 서로 어긋날 수 있으므로,
config.py를 바꾼 뒤에는 반드시 재학습 + 재export 해야 한다.
"""

# 계좌당 모델이 보는 최근 거래 개수 상한.
# 이보다 많은 이력이 있으면 "가장 최근 MAX_SEQ_LEN건"만 사용(오래된 것부터 버림).
# 이보다 적으면 앞쪽을 0-padding하고 실제 길이를 length로 함께 전달한다.
MAX_SEQ_LEN = 30

# 가맹점 카테고리 어휘. 실제 운영 카테고리 체계가 정해지면 교체.
# 인덱스 0은 "미상/OOV"로 예약.
MERCHANT_CATEGORIES = [
    "UNKNOWN",
    "GROCERY",
    "ELECTRONICS",
    "TRAVEL",
    "ONLINE_SHOPPING",
    "RESTAURANT",
    "FUEL",
    "ENTERTAINMENT",
    "FINANCIAL",
    "CASH_ADVANCE",
    "OTHER",
]
MERCHANT_TO_IDX = {name: idx for idx, name in enumerate(MERCHANT_CATEGORIES)}

# 스텝(거래 1건)당 연속형 피처 3개: amount_ratio, gap_sec, country_changed
# (모두 CP2 AccountFeatureVector와 같은 의미론 — docs 참고)
CONTINUOUS_FEATURE_DIM = 3

MERCHANT_EMBEDDING_DIM = 4
LSTM_HIDDEN_DIM = 64
LSTM_NUM_LAYERS = 1
DROPOUT = 0.1

RANDOM_SEED = 42
