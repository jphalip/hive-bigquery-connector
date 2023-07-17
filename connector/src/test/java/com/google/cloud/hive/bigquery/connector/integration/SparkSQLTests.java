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
import static com.google.cloud.hive.bigquery.connector.TestUtils.TEST_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SparkSQLTests extends IntegrationTestsBase {

  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testWhereClause(String engine, String readDataFormat) {
    DerbyDiskDB derby = new DerbyDiskDB(hive);
    initHive(engine, readDataFormat);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    // Read data with Spark SQL
    SparkSession spark = getSparkSession(derby);
    Dataset<Row> ds =
        spark.sql(String.format("SELECT * FROM default.%s WHERE number = 999", TEST_TABLE_NAME));
    Row[] rows = (Row[]) ds.collect();
    spark.stop();
    assertArrayEquals(
        new Object[] {
          new Object[] {999L, "abcd"},
        },
        simplifySparkRows(rows));
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write all types of data to BigQuery. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteAllTypes(String engine, String writeMethod) {
    DerbyDiskDB derby = new DerbyDiskDB(hive);
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    // Create the BQ table
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Insert data into the BQ table using Spark SQL
    SparkSession spark = getSparkSession(derby);
    spark.sql(
        String.join(
            "\n",
            "INSERT INTO " + ALL_TYPES_TABLE_NAME + " SELECT",
            "11,",
            "22,",
            "33,",
            "44,",
            "true,",
            "\"fixed char\",",
            "\"var char\",",
            "\"string\",",
            "CAST(\"2019-03-18\" AS DATE),",
            // Wall clock (no timezone)
            "CAST(\"2000-01-01 00:23:45.123456\" as TIMESTAMP),",
            "CAST(\"bytes\" AS BINARY),",
            "2.0,",
            "4.2,",
            "NAMED_STRUCT(",
            "  'min', CAST(-99999999999999999999999999999.999999999 AS" + " DECIMAL(38,9)),",
            "  'max', CAST(99999999999999999999999999999.999999999 AS" + " DECIMAL(38,9)),",
            "  'pi', CAST(3.14 AS DECIMAL(38,9)),",
            "  'big_pi', CAST(31415926535897932384626433832.795028841 AS" + " DECIMAL(38,9))",
            "),",
            "ARRAY(CAST (1 AS BIGINT), CAST (2 AS BIGINT), CAST (3 AS" + " BIGINT)),",
            "ARRAY(NAMED_STRUCT('i', CAST (1 AS BIGINT))),",
            "NAMED_STRUCT('float_field', CAST(4.2 AS FLOAT), 'ts_field', CAST"
                + " (\"2019-03-18 01:23:45.678901\" AS TIMESTAMP)),",
            "MAP('mykey', MAP('subkey', 999))",
            "FROM (select '1') t"));
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", ALL_TYPES_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(18, row.size()); // Number of columns
    assertEquals(11L, row.get(0).getLongValue());
    assertEquals(22L, row.get(1).getLongValue());
    assertEquals(33L, row.get(2).getLongValue());
    assertEquals(44L, row.get(3).getLongValue());
    assertTrue(row.get(4).getBooleanValue());
    assertEquals("fixed char", row.get(5).getStringValue());
    assertEquals("var char", row.get(6).getStringValue());
    assertEquals("string", row.get(7).getStringValue());
    assertEquals("2019-03-18", row.get(8).getStringValue());
    assertEquals("2000-01-01T00:23:45.123456", row.get(9).getStringValue());
    assertArrayEquals("bytes".getBytes(), row.get(10).getBytesValue());
    assertEquals(2.0, row.get(11).getDoubleValue());
    assertEquals(4.2, row.get(12).getDoubleValue());
    FieldValueList struct = row.get(13).getRecordValue();
    assertEquals(
        "-99999999999999999999999999999.999999999",
        struct.get("min").getNumericValue().toPlainString());
    assertEquals(
        "99999999999999999999999999999.999999999",
        struct.get("max").getNumericValue().toPlainString());
    assertEquals("3.14", struct.get("pi").getNumericValue().toPlainString());
    assertEquals(
        "31415926535897932384626433832.795028841",
        struct.get("big_pi").getNumericValue().toPlainString());
    FieldValueList array = (FieldValueList) row.get(14).getValue();
    assertEquals(3, array.size());
    assertEquals(1, array.get(0).getLongValue());
    assertEquals(2, array.get(1).getLongValue());
    assertEquals(3, array.get(2).getLongValue());
    FieldValueList arrayOfStructs = (FieldValueList) row.get(15).getValue();
    assertEquals(1, arrayOfStructs.size());
    struct = (FieldValueList) arrayOfStructs.get(0).getValue();
    assertEquals(1L, struct.get(0).getLongValue());
    // Mixed struct
    struct = row.get(16).getRecordValue();
    assertEquals(
        4.199999809265137,
        struct.get("float_field").getDoubleValue()); // TODO: Address discrepancy here
    assertEquals("2019-03-18T01:23:45.678901", struct.get("ts_field").getStringValue());
    // Check the Map type
    FieldValueList map = (FieldValueList) row.get(17).getRepeatedValue();
    assertEquals(1, map.size());
    FieldValueList entry = map.get(0).getRecordValue();
    assertEquals("mykey", entry.get(0).getStringValue());
    assertEquals(1, entry.get(1).getRepeatedValue().size());
    FieldValueList subEntry = entry.get(1).getRepeatedValue().get(0).getRecordValue();
    assertEquals("subkey", subEntry.get(0).getStringValue());
    assertEquals(999, subEntry.get(1).getLongValue());
  }
}
