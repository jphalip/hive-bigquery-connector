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
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PartitionIntegrationTests extends PartitionIntegrationTestsBase {

  // Note: Other tests are inherited from the parent class

  @Test
  public void testCreateIngestionTimePartition() {
    initHive();
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, INGESTION_TIME_PARTITIONED_TABLE_NAME);
    // Create the table using Hive
    createManagedTable(
        INGESTION_TIME_PARTITIONED_TABLE_NAME,
        HIVE_INGESTION_TIME_PARTITIONED_DDL,
        HIVE_INGESTION_TIME_PARTITIONED_PROPS,
        null);
    // Retrieve the table metadata from BigQuery
    StandardTableDefinition tableDef =
        getTableInfo(dataset, INGESTION_TIME_PARTITIONED_TABLE_NAME).getDefinition();
    TimePartitioning timePartitioning = tableDef.getTimePartitioning();
    assertEquals(TimePartitioning.Type.DAY, timePartitioning.getType());
    assertNull(timePartitioning.getField());
    List<Object[]> rows =
        hive.executeStatement("DESCRIBE " + INGESTION_TIME_PARTITIONED_TABLE_NAME);
    // Verify that the partition pseudo columns were added.
    assertArrayEquals(
        new Object[] {
          new Object[] {"int_val", "bigint", "from deserializer"},
          new Object[] {"_partitiontime", "timestamp with local time zone", "from deserializer"},
          new Object[] {"_partitiondate", "date", "from deserializer"}
        },
        rows.toArray());
  }

  @ParameterizedTest
  @MethodSource(READ_FORMAT)
  public void testQueryIngestionTimePartition(String readDataFormat) throws IOException {
    initHive(getDefaultExecutionEngine(), readDataFormat);
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, INGESTION_TIME_PARTITIONED_TABLE_NAME);
    // Create the table using Hive
    createManagedTable(
        INGESTION_TIME_PARTITIONED_TABLE_NAME,
        HIVE_INGESTION_TIME_PARTITIONED_DDL,
        HIVE_INGESTION_TIME_PARTITIONED_PROPS,
        null);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` (_PARTITIONTIME, int_val) VALUES "
                + "(TIMESTAMP_TRUNC('2017-05-01', DAY), 123),"
                + "(TIMESTAMP_TRUNC('2021-06-12', DAY), 999)",
            INGESTION_TIME_PARTITIONED_TABLE_NAME));
    List<Object[]> rows =
        runHiveQuery(
            String.format(
                "SELECT `int_val`, `_partitiontime`, `_partitiondate` from %s WHERE `_partitiondate` <= '2019-08-02'",
                INGESTION_TIME_PARTITIONED_TABLE_NAME));
    assertEquals(1, rows.size());
    Object[] row = rows.get(0);
    assertEquals(3, row.length); // Number of columns
    assertEquals(123L, row[0]);
    assertEquals(
        "2017-05-01T00:00:00Z", // 'Z' == UTC
        Instant.from(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S VV", Locale.getDefault())
                    .parse(row[1].toString()))
            .toString());
    assertEquals("2017-05-01", row[2]);
  }
}
