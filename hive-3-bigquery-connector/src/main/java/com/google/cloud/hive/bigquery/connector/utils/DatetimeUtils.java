package com.google.cloud.hive.bigquery.connector.utils;

import java.time.*;

import com.google.cloud.bigquery.storage.v1beta2.CivilTimeEncoder;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;

public class DatetimeUtils {

    public static TimestampTZ getHiveTimestampTZFromUTC(long utc) {
        long seconds = utc / 1_000_000;
        int nanos = (int) (utc % 1_000_000) * 1_000;
        ZonedDateTime zonedDateTime = Instant.ofEpochSecond(seconds, nanos).atZone(ZoneId.of("UTC"));
        return new TimestampTZ(zonedDateTime);
    }

    public static long getEpochMicrosFromHiveTimestampTZ(TimestampTZ timestampTZ) {
        return timestampTZ.getZonedDateTime().toEpochSecond() * 1_000_000
            + timestampTZ.getZonedDateTime().getNano() / 1_000;
    }

    public static long getEpochMicrosFromHiveTimestamp(Timestamp timestamp) {
        return timestamp.toEpochSecond() * 1_000_000 + timestamp.getNanos() / 1_000;
    }

    public static Timestamp getHiveTimestampFromLocalDatetime(LocalDateTime localDateTime) {
        return Timestamp.ofEpochSecond(
            localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
    }

    public static long getEncodedProtoLongFromHiveTimestamp(Timestamp timestamp) {
        return CivilTimeEncoder.encodePacked64DatetimeMicros(
            org.threeten.bp.LocalDateTime.of(
                timestamp.getYear(),
                timestamp.getMonth(),
                timestamp.getDay(),
                timestamp.getHours(),
                timestamp.getMinutes(),
                timestamp.getSeconds(),
                timestamp.getNanos()));
    }

}
