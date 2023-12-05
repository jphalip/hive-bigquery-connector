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
package com.google.cloud.hive.bigquery.connector.sparksql;

import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class SparkUtils {

  public static final String SPARK_JOB_FILE_NAME = "spark-job.json";

  public static void writeSparkJobFile(
      Configuration conf, List<String> insertTables, List<String> overwriteTables) {
    try {
      Map<String, Object> data = new HashMap<>();
      data.put("insertTables", insertTables);
      data.put("overwriteTables", overwriteTables);
      Path path = new Path(JobUtils.getQueryWorkDir(conf), SPARK_JOB_FILE_NAME);
      FSDataOutputStream jobFile = path.getFileSystem(conf).create(path);
      Gson gson = new Gson();
      jobFile.write(gson.toJson(data).getBytes(StandardCharsets.UTF_8));
      jobFile.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
