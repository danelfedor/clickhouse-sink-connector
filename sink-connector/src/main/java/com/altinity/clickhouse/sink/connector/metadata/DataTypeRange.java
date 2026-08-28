package com.altinity.clickhouse.sink.connector.metadata;

import com.clickhouse.data.format.BinaryStreamUtils;

import java.time.*;

import static java.time.Instant.from;
import static java.time.Instant.ofEpochMilli;

public class DataTypeRange
{

    // Set clickhouse-jdbc limits
    public static final Integer CLICKHOUSE_MIN_SUPPORTED_DATE32 = BinaryStreamUtils.DATE32_MIN;

    public static final Integer CLICKHOUSE_MAX_SUPPORTED_DATE32 = BinaryStreamUtils.DATE32_MAX;


    // DateTime64 内部是Int64, 范围由精度决定: DateTime64(3)可表示±2.9亿年, DateTime64(6)±29万年.
    // 上限/下限设为SQL Server datetime2 的范围(0001-01-01 ~ 9999-12-31), 覆盖源库全部可能值.
    public static final long DATETIME64_MAX = LocalDateTime.of(LocalDate.of(9999, 12, 31), LocalTime.MAX).toEpochSecond(ZoneOffset.UTC);
    public static final long DATETIME64_MIN = LocalDateTime.of(LocalDate.of(1, 1, 1), LocalTime.MIN).toEpochSecond(ZoneOffset.UTC);

    // DateTime
    public static final Instant CLICKHOUSE_MIN_SUPPORTED_DATETIME64 = from(ofEpochMilli
            (DATETIME64_MIN * 1000).atZone(ZoneId.of("UTC"))).plusNanos(DATETIME64_MIN * 1000 % 1_000);
    public static final Instant CLICKHOUSE_MAX_SUPPORTED_DATETIME64 = from(ofEpochMilli
            (DATETIME64_MAX * 1000).atZone(ZoneId.of("UTC")).withHour(23).withMinute(59).withSecond(59).withNano(000000));


    // DateTime and DateTime32
    public static final long DATETIME32_MIN = 0L;
    public static final long DATETIME32_MAX = LocalDateTime.of(LocalDate.of(2106, 02, 07), LocalTime.of(6, 28, 15)).toEpochSecond(ZoneOffset.UTC);

    // DateTime max limits.
    public static final String DATETIME_MIN="1900-01-01 00:00:00.0";
    public static final String DATETIME_MAX="2299-12-31 23:59:59.0";

    public static final String DATETIME_1_MAX = "2299-12-31 23:59:59.0";

    public static final String DATETIME_2_MAX = "2299-12-31 23:59:59.0";

    public static final String DATETIME_3_MAX = "2299-12-31 23:59:59.0";

    public static final String DATETIME_4_MAX = "2299-12-31 23:59:59.0";

    public static final String DATETIME_5_MAX = "2299-12-31 23:59:59.0";

    public static final String DATETIME_6_MAX = "2299-12-31 23:59:59.0";

    public static final String DATETIME64_6_MAX = "2299-12-31 23:59:59.0";

}
