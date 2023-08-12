package com.google.cloud.hive.bigquery.connector;

import java.util.List;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class BigQueryMetaHook extends BigQueryMetaHookBase {

    public BigQueryMetaHook(Configuration conf) {
        super(conf);
    }

    @Override
    protected void setupIngestionTimePartitioning(Table table) throws MetaException {
        // Add the BigQuery pseudo columns to the Hive MetaStore schema.
        assertDoesNotContainColumn(table, HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN);
        table
            .getSd()
            .addToCols(
                new FieldSchema(
                    HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN,
                    "timestamp with local time zone",
                    "Ingestion time pseudo column"));
        assertDoesNotContainColumn(table, HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN);
        table
            .getSd()
            .addToCols(
                new FieldSchema(
                    HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN,
                    "date",
                    "Ingestion time pseudo column"));
    }

    @Override
    protected void setupStats(Table table) {
        StatsSetupConst.setStatsStateForCreateTable(
            table.getParameters(), null, StatsSetupConst.FALSE);
    }

    @Override
    protected List<PrimitiveObjectInspector.PrimitiveCategory> getSupportedTypes() {
        List<PrimitiveObjectInspector.PrimitiveCategory> supportedTypes = super.getSupportedTypes();
        supportedTypes.add(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ);
        return supportedTypes;
    }

}
