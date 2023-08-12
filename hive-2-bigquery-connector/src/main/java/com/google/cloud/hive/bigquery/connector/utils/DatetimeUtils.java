package com.google.cloud.hive.bigquery.connector.utils;

import com.google.cloud.bigquery.storage.v1beta2.CivilTimeEncoder;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class DatetimeUtils {

    public static Timestamp getHiveTimestampFromLocalDatetime(LocalDateTime localDateTime) {
        return Timestamp.valueOf(localDateTime);
    }

    public static long getEpochMicrosFromHiveTimestamp(Timestamp timestamp) {
        return timestamp.toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1_000_000
            + timestamp.getNanos() / 1_000;
    }

    public static long getEncodedProtoLongFromHiveTimestamp(Timestamp timestamp) {
        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        return CivilTimeEncoder.encodePacked64DatetimeMicros(
            org.threeten.bp.LocalDateTime.of(
                localDateTime.getYear(),
                localDateTime.getMonthValue(),
                localDateTime.getDayOfMonth(),
                localDateTime.getHour(),
                localDateTime.getMinute(),
                localDateTime.getSecond(),
                localDateTime.getNano()));
    }

}
