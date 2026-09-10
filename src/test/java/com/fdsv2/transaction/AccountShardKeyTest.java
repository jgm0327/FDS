package com.fdsv2.transaction;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class AccountShardKeyTest {

    @Test
    void format과_parse는_서로_역함수다() {
        String formatted = AccountShardKey.format("acc-1", 3);
        assertThat(formatted).isEqualTo("acc-1#3");

        AccountShardKey parsed = AccountShardKey.parse(formatted);
        assertThat(parsed.accountId()).isEqualTo("acc-1");
        assertThat(parsed.shardIndex()).isEqualTo(3);
    }

    @Test
    void 구분자가_없는_옛_포맷도_shardIndex_0으로_방어적으로_파싱한다() {
        AccountShardKey parsed = AccountShardKey.parse("acc-legacy");
        assertThat(parsed.accountId()).isEqualTo("acc-legacy");
        assertThat(parsed.shardIndex()).isEqualTo(0);
    }

    @Test
    void 샤드_인덱스_부분이_숫자가_아니면_통째로_accountId로_취급한다() {
        AccountShardKey parsed = AccountShardKey.parse("weird#account#name");
        assertThat(parsed.accountId()).isEqualTo("weird#account#name");
        assertThat(parsed.shardIndex()).isEqualTo(0);
    }
}
