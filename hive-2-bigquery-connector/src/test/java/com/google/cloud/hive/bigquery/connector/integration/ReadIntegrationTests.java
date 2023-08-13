package com.google.cloud.hive.bigquery.connector.integration;


import java.io.IOException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ReadIntegrationTests extends ReadIntegrationTestsBase {

    @ParameterizedTest
    @MethodSource(READ_FORMAT)
    public void testReadAllTypesHive2(String readDataFormat) throws IOException {
        readAllTypes(readDataFormat, null);
    }

}
