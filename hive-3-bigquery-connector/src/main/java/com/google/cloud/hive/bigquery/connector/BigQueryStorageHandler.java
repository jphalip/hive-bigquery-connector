package com.google.cloud.hive.bigquery.connector;

import java.util.Map;

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQueryUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.hive.ql.stats.Partish;

public class BigQueryStorageHandler extends BigQueryStorageHandlerBase {

  // @Override
  public Map<String, String> getBasicStatistics(Partish partish) {
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = partish.getTable();
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, hmsTable.getParameters()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    HiveBigQueryConfig config = injector.getInstance(HiveBigQueryConfig.class);
    return BigQueryUtils.getBasicStatistics(bqClient, config.getTableId());
  }

}
