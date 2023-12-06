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

import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;

public class ExecHookUtils {

  public static boolean isWritingToBqTable(HookContext hookContext) {
    // First, check if we're indeed processing a BigQuery table
    for (WriteEntity entity : hookContext.getOutputs()) {
      if (entity.getWriteType() != WriteType.INSERT
          && entity.getWriteType() != WriteType.INSERT_OVERWRITE) {
        continue;
      }
      if (entity
          .getTable()
          .getStorageHandler()
          .getClass()
          .getName()
          .equals("com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler")) {
        return true;
      }
    }
    return false;
  }
}
