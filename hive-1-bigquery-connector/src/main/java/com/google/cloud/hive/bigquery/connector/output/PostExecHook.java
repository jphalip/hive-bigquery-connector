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
package com.google.cloud.hive.bigquery.connector.output;

import com.google.cloud.hive.bigquery.connector.BigQueryMetaHook;
import com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;

/**
 * Post execution hook used to commit the outputs. We only use this with Hive 1.x.x
 * in combination with Tez.
 */
public class PostExecHook implements ExecuteWithHookContext {

  @Override
  public void run(HookContext hookContext) throws Exception {
    for (WriteEntity entity : hookContext.getOutputs()) {
      if (!entity.getTable().getStorageHandler().getClass().equals(BigQueryStorageHandler.class)) {
        // Not a BigQuery table, so skip it
        continue;
      }
      String tableName = HiveUtils.getDbTableName(entity.getTable());
      BigQueryMetaHook metahook = new BigQueryMetaHook(hookContext.getConf());
      metahook.commitInsertTable(tableName);
    }
  }
}
