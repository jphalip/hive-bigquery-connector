package com.google.cloud.hive.bigquery.connector;

import org.junit.jupiter.api.extension.Extension;

public class SystemPropertyOverridesExtension implements Extension {

  public SystemPropertyOverridesExtension() {
//    System.setProperty("hiveconf_datanucleus.connectionPoolingType", "dbcp-builtin");
//    System.setProperty("hiveconf_datanucleus.connectionPoolingDBCP.maxPoolSize", "1");
//    System.setProperty("hiveconf_datanucleus.connectionPoolingDBCP.minIdle", "1");
//    System.setProperty("hiveconf_datanucleus.connectionPoolingDBCP.maxIdle", "1");
  }

}
