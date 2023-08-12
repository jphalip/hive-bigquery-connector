package com.google.cloud.hive.bigquery.connector;

import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.plan.TableDesc;

public class BigQueryStorageHandler extends BigQueryStorageHandlerBase {

    @Override
    public HiveMetaHook getMetaHook() {
        return new BigQueryMetaHook(conf);
    }

}
