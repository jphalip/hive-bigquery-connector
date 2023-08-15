/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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
package com.google.cloud.hive.bigquery.connector.integration;

import static com.google.cloud.hive.bigquery.connector.TestUtils.*;
import static com.google.cloud.hive.bigquery.connector.TestUtils.ALL_TYPES_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ReadIntegrationTests extends ReadIntegrationTestsBase {

  @ParameterizedTest
  @MethodSource(READ_FORMAT)
  public void testReadTimeStampTZ(String readDataFormat) {
    initHive(getDefaultExecutionEngine(), readDataFormat);
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Insert data into the BQ table using the BQ SDK
    String query =
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES (", ALL_TYPES_TABLE_NAME),
            // (Pacific/Honolulu, -10:00)
            "cast(\"2000-01-01T00:23:45.123456-10\" as timestamp)",
    ")");
    runBqQuery(query);
    // Read the data using Hive
    List<Object[]> rows = runHiveQuery("SELECT * FROM " + ALL_TYPES_TABLE_NAME);
    assertEquals(1, rows.size());
    Object[] row = rows.get(0);
    assertEquals(1, row.length); // Number of columns
    assertEquals(
        "2000-01-01T10:23:45.123456Z", // 'Z' == UTC
        Instant.from(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS VV")
                    .parse(row[0].toString()))
            .toString());
  }
}
