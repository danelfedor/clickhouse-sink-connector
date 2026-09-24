package com.altinity.clickhouse.sink.connector.common;

import java.util.BitSet;

/**
 * Snowflake ID implementation
 * 41 bits(timestamp in milliseconds, 减 SNOWFLAKE_EPOCH; ignoreSnowflakeEpoch=true 时用原始毫秒)
 * 22 bits(低位值, 只取低 22 位: gtid 或"毫秒内序号")
 *
 * 注意: 这里只借用 snowflake 的位布局, 没有机器号(worker id)位, 也没有"每毫秒重置的
 * 12 位序列"——低位放什么、是否按毫秒归零, 由调用方决定, 且超出 22 位的部分会被静默截断。
 * https://en.wikipedia.org/wiki/Snowflake_ID
 */

public class SnowFlakeId {

    private static final long SNOWFLAKE_EPOCH = 1288834974657L;

    /**
     * @param timestamp            毫秒时间戳
     * @param lowBits              gtid 或序号; 只保留低 22 位, 调用方需保证它是有界的
     * @param ignoreSnowflakeEpoch true = 直接用原始毫秒(不减 SNOWFLAKE_EPOCH)
     */
    public static long generate(long timestamp, long lowBits, boolean ignoreSnowflakeEpoch) {
        // 1. Create bitset with 64 bits
        BitSet result = new BitSet(64);

        // 2. Create bitset from long (timestamp) - 41 bits
        long tsDiff = timestamp - SNOWFLAKE_EPOCH;
        BitSet tsBitSet = BitSet.valueOf(new long[] {tsDiff});
        if(ignoreSnowflakeEpoch) {
            tsBitSet = BitSet.valueOf(new long[] {timestamp});
        }

        // 3. Create bitset from lowBits - 22 bits
        BitSet lowBitsSet = BitSet.valueOf(new long[] {lowBits});
        BitSet low22Bits = lowBitsSet.get(0, 22);

        BitSet ts41Bits = tsBitSet.get(0, 41);

        // Set lowBits in result.
        for(int i = 0; i <= 21; i++) {
            result.set(i, low22Bits.get(i));
        }
        int tsIndex = 0;
        for(int j = 22; j <= 62; j++) {
            result.set(j, ts41Bits.get(tsIndex++));
        }
        return result.toLongArray()[0];
    }
}
