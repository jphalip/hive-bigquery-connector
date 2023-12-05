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
package com.google.cloud.hive.bigquery.connector.utils.sparksql;

import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SparkSQLUtils {

  public static final String SPARK_JOB_FILE_NAME = "spark-job.json";

  public static boolean isSparkJob(Configuration conf) {
    return conf.get("spark.app.id", "").length() != 0;
  }

  public static void cleanUpSparkJobFile(Configuration conf) throws IOException {
    Path path = new Path(JobUtils.getQueryWorkDir(conf), SPARK_JOB_FILE_NAME);
    FileSystem fileSystem = path.getFileSystem(conf);
    fileSystem.delete(path, true);
  }

  public static Map<String, Object> readSparkJobFile(Configuration conf) {
    Path path = new Path(JobUtils.getQueryWorkDir(conf), "spark-job.json");
    String jsonString;
    try {
      jsonString = FileSystemUtils.readFile(conf, path);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Could not find %s file. Make sure to set the Hive-BigQuery Spark SQL extension class in the `spark.sql.extensions` configuration property",
              SPARK_JOB_FILE_NAME));
    }
    Gson gson = new Gson();
    return gson.fromJson(jsonString, HashMap.class);
  }

  public static boolean isOverwrite(Configuration conf, String tableName) {
    Map<String, Object> sparkInfo = readSparkJobFile(conf);
    List<String> overwriteTables = (List<String>) sparkInfo.get("overwriteTables");
    return overwriteTables.contains(tableName);
  }
}
