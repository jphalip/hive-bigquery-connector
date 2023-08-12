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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class HiveVersionCondition implements ExecutionCondition {

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (context.getElement().isPresent()) {
      if (context.getElement().get().isAnnotationPresent(EnabledIfHive2.class)) {
        return TestUtils.isHive2()
            ? ConditionEvaluationResult.enabled("Test is enabled for Hive 2.x")
            : ConditionEvaluationResult.disabled("Test is not applicable for Hive 2.x");
      }

      if (context.getElement().get().isAnnotationPresent(EnabledIfHive3.class)) {
        return TestUtils.isHive3()
            ? ConditionEvaluationResult.enabled("Test is enabled for Hive 3.x")
            : ConditionEvaluationResult.disabled("Test is not applicable for Hive 3.x");
      }
    }
    return ConditionEvaluationResult.enabled("Test doesn't have a specific Hive version condition");
  }
}
