package com.google.cloud.hive.bigquery.connector;


import org.apache.hive.common.util.HiveVersionInfo;
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
