# FDS v2 하네스 엔지니어링 가이드

## 프로젝트 목표

기존 FDS(Fraud Detection System)는 기간 제약 때문에 **단건 거래**만으로 이상 여부를 판단하는 한계가 있었다.
이번 재설계는 기간 제약 없이, 실제 운영 환경과 유사한 수준으로 **다중 거래 시퀀스 기반** 판단을 목표로 한다.

핵심 전환점: "이 거래 하나가 이상한가?" → "이 계좌의 최근 거래 흐름이 이상한가?"

## 핵심 설계 원칙 (2026-09-03 논의 확정)

1. **파티셔닝**: Kafka 프로듀서에서 계좌ID를 파티션 키로 사용. 같은 계좌의 거래는 항상 같은 파티션 → 같은 Kafka Streams 태스크로 가야 시퀀스 순서가 보장됨.
2. **실시간 시퀀스 집계**: Kafka Streams가 계좌별 슬라이딩 윈도우 통계(최근 거래 횟수, 평소 대비 금액 배율, 마지막 거래 이후 경과 시간 등)를 State Store(RocksDB, 임베디드)에서 관리. 모든 변경사항은 changelog topic에 백업되어 장애 시 복구 가능.
3. **온라인 피처 스토어**: 계산이 끝난 최종 요약값(피처 벡터)만 Redis에 계좌ID 키로 저장. 새 거래마다 덮어쓰며, TTL(예: 30분)로 비활성 계좌 데이터를 자동 정리.
4. **모델 서빙**: PyTorch LSTM/Transformer를 TorchServe로 서빙. Spring Boot가 비동기 호출하며, 타임아웃 시 규칙 기반 기본 스코어로 폴백(Circuit Breaker 연계).
5. **판정**: 모델 확률 + 규칙 엔진(하드룰, 화이트리스트)을 앙상블. 하드룰은 OR로 즉시 반영, 애매한 구간은 가중합. 최종 액션은 허용/추가인증/차단 3단계.
6. **피드백 루프**: 판정 결과와 추후 확정되는 사기/정상 라벨을 함께 로깅 → 재학습 파이프라인의 입력으로 사용.

## 작업 방식 — 병렬 worktree + 세션 로그 규칙

이 프로젝트는 **git worktree 기반 병렬 작업**으로 진행한다 (Orca 등으로 오케스트레이션).

- 브랜치 네이밍: AI/모델 관련 작업은 `ai/<기능명>`, 백엔드 작업은 `backend/<기능명>`
  - 예: `ai/pytorch-sequence-model`, `backend/kafka-partitioning`, `backend/redis-feature-store`
- 각 브랜치는 독립된 worktree에서 작업하며, 서로 파일을 건드리지 않는 범위로 기능을 쪼갠다.

**매 작업 세션이 끝날 때마다 `docs/sessions/`에 세션 로그를 작성한다.** 병렬 작업이므로 파일명에 브랜치명을 반드시 포함한다.

- 파일명: `docs/sessions/YYYY-MM-DD_<브랜치명>_session-NN.md`
  - 예: `docs/sessions/2026-09-03_backend-kafka-partitioning_session-01.md`
- 템플릿: `docs/SESSION_LOG_TEMPLATE.md` 참고
- 작업이 끝나면 해당 브랜치에서 PR을 생성한다 (커밋/PR에 AI 도구 표시가 남지 않도록 attribution 설정을 끈 상태로 진행 — `docs/WORKTREE_SETUP.md` 참고)
- 반드시 포함할 내용:
  - 이번 세션에서 다룬 질문/요청
  - 그로 인해 바뀌거나 새로 만들어진 내용 (코드, 설계, 문서)
  - 어떤 생각/근거로 그렇게 설계했는지 (트레이드오프 포함)
  - 막혔던 문제와 어떻게 해결했는지
  - 다음에 이어서 할 일

이 로그들은 나중에 면접에서 "왜 이렇게 설계했는지"를 설명할 때 그대로 근거 자료로 쓸 수 있어야 한다 — 결과뿐 아니라 **사고 과정**을 남기는 것이 목적.

## 프로젝트 독립성

이 프로젝트(FDS v2)는 기존 FDS 하네스 문서(CLAUDE.md, BACKEND.md, INFRA.md, PERFORMANCE.md, MEASUREMENT_CHECKPOINTS.md)와는
**별개의 새 프로젝트**로 시작한다. 파일명이 겹치더라도 서로 다른 폴더에서 독립적으로 관리하며, 내용을 병합하지 않는다.

## 폴더 구조

```
fds-v2/
├── CLAUDE.md                          # 이 파일 — 프로젝트 목표, 작업 규칙
├── ai/                                 # ai/** 브랜치 작업 결과물 (PyTorch 모델 등)
├── backend/                            # backend/** 브랜치 작업 결과물 (Kafka, Redis, Spring Boot 등)
└── docs/
    ├── ARCHITECTURE.md                # 전체 파이프라인 상세 설계
    ├── PERFORMANCE_MEASUREMENT.md     # CP1~CP5 지표, k6 시나리오
    ├── BACKEND.md                     # 백엔드 구현 스펙 (Claude Code 참고용)
    ├── WORKTREE_SETUP.md              # 병렬 worktree/PR 작업 가이드
    ├── SESSION_LOG_TEMPLATE.md        # 세션 로그 작성 템플릿
    └── sessions/
        ├── 2026-09-03_session-01.md              # (병렬화 이전) 설계 논의
        ├── 2026-09-03_session-02.md              # (병렬화 이전) TODO 판단, 구현 시작
        └── 2026-09-03_<브랜치명>_session-NN.md    # 병렬 작업 이후 형식
```

## 참고 문서

- `docs/ARCHITECTURE.md` — 전체 파이프라인 상세 설계
- `docs/PERFORMANCE_MEASUREMENT.md` — CP1~CP5 단위 Prometheus/Grafana 지표 및 k6 부하테스트 시나리오
- `docs/BACKEND.md` — 백엔드 구현 스펙 (실제 코드는 Claude Code가 이 스펙을 보고 작성)
- `docs/WORKTREE_SETUP.md` — 병렬 worktree 생성, 브랜치 네이밍, PR 생성 가이드
- `docs/SESSION_LOG_TEMPLATE.md` — 세션 로그 작성 템플릿
- `docs/sessions/` — 날짜/브랜치별 세션 로그 모음
