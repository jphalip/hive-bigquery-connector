/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hive.bigquery.connector.utils;

import com.google.cloud.bigquery.storage.v1beta2.CivilTimeEncoder;
import org.apache.hadoop.hive.common.type.TimestampTZ;

import java.sql.Timestamp;
import java.time.*;

public class DateTimeUtils {

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

  public static Timestamp getHiveTimestampFromLocalDatetime(LocalDateTime localDateTime) {
    return Timestamp.valueOf(localDateTime);
  }
}
