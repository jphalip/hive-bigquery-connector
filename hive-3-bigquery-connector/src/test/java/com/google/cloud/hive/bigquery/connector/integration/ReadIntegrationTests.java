package com.google.cloud.hive.bigquery.connector.integration;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ReadIntegrationTests extends ReadIntegrationTestsBase {

    @ParameterizedTest
    @MethodSource(READ_FORMAT)
    public void testReadAllTypesHive(String readDataFormat) throws IOException {
        // (Pacific/Honolulu, -10:00)
        String additionalCol = "cast(\"2000-01-01T00:23:45.123456-10\" as timestamp)";
        Object[] row = readAllTypes(readDataFormat, additionalCol);
        assertEquals(
            "2000-01-01T10:23:45.123456Z", // 'Z' == UTC
            Instant.from(
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS VV")
                        .parse(row[18].toString()))
                .toString());
    }

}
