# 병렬 Worktree 작업 가이드

이 문서는 **로컬 환경에서 직접 실행**하는 가이드다. Claude(채팅)는 실제 저장소에 접근할 수 없으므로,
Claude Code CLI + git + (선택) Orca + GitHub CLI(`gh`)로 아래 흐름을 로컬에서 실행한다.

## 1. Claude 표시가 남지 않도록 사전 설정

`~/.claude/settings.json`에 추가:

```json
{
  "includeCoAuthoredBy": false,
  "attribution": {
    "commit": "",
    "pr": ""
  }
}
```

일부 경로(Bash 도구로 직접 `git commit` 실행 시)에서 이 설정이 무시되는 사례가 보고되어 있으므로,
안전하게 커밋 훅으로 한 번 더 필터링한다.

```bash
cat > .git/hooks/commit-msg << 'EOF'
#!/usr/bin/env bash
sed -i '' '/Generated with \[Claude Code\]/d' "$1"
sed -i '' '/Co-Authored-By: Claude/d' "$1"
sed -i '' '/Claude-Session:/d' "$1"
EOF
chmod +x .git/hooks/commit-msg
```

## 2. 브랜치 네이밍 규칙

- AI/모델 관련: `ai/<기능명>` (예: `ai/pytorch-sequence-model`, `ai/feature-engineering`)
- 백엔드 관련: `backend/<기능명>` (예: `backend/kafka-partitioning`, `backend/redis-feature-store`)

## 3. Worktree 생성 (기능별로 반복)

```bash
# 백엔드 - Kafka 파티셔닝 작업
git worktree add ../fds-v2-backend-kafka backend/kafka-partitioning

# AI - PyTorch 시퀀스 모델 작업
git worktree add ../fds-v2-ai-model ai/pytorch-sequence-model
```

각 worktree 디렉토리에서 `claude` (Claude Code CLI)를 실행하면, 해당 브랜치/디렉토리에만 격리되어 작업한다.
Orca를 쓴다면 `orca pod create`로 여러 worktree를 한 번에 정의하고 상태를 추적할 수 있다.

## 4. 작업 종료 후 세션 로그 + PR

각 worktree에서 작업이 끝나면:

1. `docs/SESSION_LOG_TEMPLATE.md` 형식으로 `docs/sessions/YYYY-MM-DD_<브랜치명>_session-NN.md` 작성 후 커밋
2. PR 생성 (attribution 설정이 적용된 상태이므로 PR 본문에 Claude 표시가 남지 않음)

```bash
gh pr create \
  --base main \
  --head backend/kafka-partitioning \
  --title "feat: Kafka 거래 이벤트 파티셔닝 구현" \
  --body "$(cat docs/sessions/2026-09-03_backend-kafka-partitioning_session-01.md)"
```

## 5. 작업 완료 후 worktree 정리

```bash
git worktree remove ../fds-v2-backend-kafka
git worktree prune
```

Claude Code는 세션이 비정상 종료되면 worktree를 자동으로 정리하지 못하는 경우가 있으므로,
PR 병합 후에는 수동으로 위 명령을 실행해 정리한다.

## 병렬 진행 시 권장 기능 분리 (독립적으로 진행 가능)

| 브랜치 | 내용 | 의존성 |
|---|---|---|
| `backend/kafka-partitioning` | 프로듀서/컨슈머, 파티션 검증 | 없음 |
| `backend/kafka-streams-topology` | 슬라이딩 윈도우 집계 | Kafka 토픽 스키마만 필요 |
| `backend/redis-feature-store` | Redis 연동, TTL | 피처 벡터 형식 합의만 필요 |
| `ai/pytorch-sequence-model` | 모델 구조, 학습 스크립트 | 없음 (가짜 데이터로 병렬 진행 가능) |
| `backend/model-client` | ModelInferenceClient (REST) | AI 쪽 모델 입출력 스펙만 필요 |

판정/앙상블 로직(`backend/decision-ensemble`)은 위 항목들이 합쳐진 뒤 마지막에 순차로 진행한다.
