package com.google.cloud.hive.bigquery.connector;

import org.apache.hadoop.hive.metastore.HiveMetaHook;

public class BigQueryStorageHandler extends BigQueryStorageHandlerBase {

    @Override
    public HiveMetaHook getMetaHook() {
        return new BigQueryMetaHook(conf);
    }

}
