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
package com.google.cloud.hive.bigquery.connector;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

/**
 * Implementation of the Hive MetaHook that inherits from `DefaultHiveMetaHook`, which is available
 * in newer versions of Hive. This allows to use methods like `commitInsertTable()`.
 */
public class NewAPIMetaHook extends DefaultHiveMetaHook implements MetahookExtension {

  BigQueryMetaHook metahook;

  public NewAPIMetaHook(Configuration conf) {
    this.metahook = new BigQueryMetaHook(conf, this);
  }

  @Override
  public void setupIngestionTimePartitioning(Table table) throws MetaException {
    throw new MetaException(
        "Ingestion-time partitioned tables are not supported in Hive versions < 3.x.x");
  }

  @Override
  public void setupStats(Table table) {
    // Do nothing
  }

  @Override
  public List<PrimitiveCategory> getSupportedTypes() {
    return new ArrayList<>(BigQueryMetaHook.basicTypes);
  }

  @Override
  public void commitInsertTable(Table table, boolean overwrite) throws MetaException {
    metahook.commitInsertTable(table, overwrite);
  }

  @Override
  public void preInsertTable(Table table, boolean overwrite) throws MetaException {
    metahook.preInsertTable(table, overwrite);
  }

  @Override
  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {
    metahook.rollbackInsertTable(table, overwrite);
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    metahook.preCreateTable(table);
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    metahook.rollbackCreateTable(table);
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    metahook.commitCreateTable(table);
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    metahook.preDropTable(table);
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    metahook.rollbackDropTable(table);
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    metahook.commitDropTable(table, deleteData);
  }
}
