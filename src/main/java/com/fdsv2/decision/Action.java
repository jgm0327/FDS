package com.fdsv2.decision;

/**
 * CP5 최종 액션 (docs/ARCHITECTURE.md 5번) — 이진 판정이 아닌 3단계로 오탐을 완충한다.
 */
public enum Action {
    ALLOW,
    STEP_UP_AUTH,
    BLOCK
}
