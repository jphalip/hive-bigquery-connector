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
package com.google.cloud.hive.bigquery.connector.integration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class WriteIntegrationTests extends WriteIntegrationTestsBase {

  // Other tests are from the super-class

  @Disabled("Insert overwrite is currently not supported for Hive 1")
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testOverwriteITAS(String engine, String writeMethod) {}

  @Disabled("Insert overwrite is currently not supported for Hive 1")
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testInsertOverwrite(String engine, String writeMethod) {}
}
