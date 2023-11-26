package org.apache.hadoop.hive.conf;

import java.util.Map;

/**
 * Hive 2 & 3 have a HiveVariableSource interface but Hive 1 doesn't. HiveRunner requires it, so we
 * define it here.
 */

public interface HiveVariableSource {
  Map<String, String> getHiveVariable();
}
