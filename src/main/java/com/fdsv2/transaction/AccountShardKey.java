package com.fdsv2.transaction;

/**
 * transaction-events 토픽의 Kafka 메시지 키 형식 — {@code "{accountId}#{shardIndex}"}.
 *
 * CP1(프로듀서, salting 대상 고빈도 계좌만 shardIndex를 0~N-1 중 무작위로 분산)과 CP2(Kafka
 * Streams, 이 키를 파싱해서 계좌ID/샤드 인덱스로 나눔) 양쪽이 공유하는 계약이라 한 클래스에만
 * 둔다 — 구분자가 둘 사이에서 어긋나면 핫 계좌 판정 자체가 깨진다.
 *
 * <p>고빈도로 지정되지 않은 일반 계좌는 항상 shardIndex=0으로 고정된다 — "accountId#0"이라는
 * 같은 문자열이 항상 같은 해시로 같은 파티션에 가므로, "같은 계좌 -> 같은 파티션" 순서 보장
 * (docs/ARCHITECTURE.md 1번)은 그대로 유지된다. CP2는 이 경우 샤드가 1개뿐인 것으로 취급해서
 * 병합 로직이 기존(샤드 없던 시절) 단일 계좌 집계와 수학적으로 동일한 결과를 낸다
 * (AccountActivityMergeProcessorTest 참고).
 */
public record AccountShardKey(String accountId, int shardIndex) {

    private static final char SEPARATOR = '#';

    public static String format(String accountId, int shardIndex) {
        return accountId + SEPARATOR + shardIndex;
    }

    /**
     * 방어적 처리: 구분자가 없는 옛 포맷(순수 accountId)이 섞여 들어와도(예: 마이그레이션 중
     * 남은 오프셋, 수동 테스트 등) 예외 없이 shardIndex=0(비-salting 취급)으로 파싱한다.
     */
    public static AccountShardKey parse(String rawKey) {
        int sep = rawKey.lastIndexOf(SEPARATOR);
        if (sep < 0) {
            return new AccountShardKey(rawKey, 0);
        }
        try {
            return new AccountShardKey(rawKey.substring(0, sep), Integer.parseInt(rawKey.substring(sep + 1)));
        } catch (NumberFormatException e) {
            // "#"이 accountId 자체에 우연히 포함된 경우(예: 외부 계좌ID 체계) 등 — 통째로
            // accountId로 취급하고 shardIndex=0.
            return new AccountShardKey(rawKey, 0);
        }
    }
}
