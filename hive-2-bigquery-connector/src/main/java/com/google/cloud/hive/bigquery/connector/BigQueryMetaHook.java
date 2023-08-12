package com.google.cloud.hive.bigquery.connector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

public class BigQueryMetaHook extends BigQueryMetaHookBase {

    public BigQueryMetaHook(Configuration conf) {
        super(conf);
    }

    @Override
    protected void setupIngestionTimePartitioning(Table table) throws MetaException {
        throw new MetaException("Ingestion-time partitioned tables are not supported in Hive v2.");
    }

    @Override
    protected void setupStats(Table table) {
        // Do nothing
    }

}
