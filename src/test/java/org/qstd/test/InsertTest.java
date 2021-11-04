/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Copyright 2021-2021 the original author or authors.
 */

package org.qstd.test;

import org.junit.jupiter.api.Test;
import org.quickperf.sql.annotation.ExpectJdbcQueryExecution;
import org.qstd.QuickSqlTestData;

import static org.assertj.core.api.Assertions.assertThat;

public class InsertTest extends H2Config {

    @Test public void
    should_generate_an_empty_insert_script_for_an_insert_input() {
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        String anInsertStatement = "INSERT INTO A_TABLE VALUES(1, 2, 3)";
        String insertScript = quickSqlTestData.generateInsertScriptFor(anInsertStatement);
        assertThat(insertScript).isEmpty();
    }

    @Test @ExpectJdbcQueryExecution(0) public void
    should_not_use_jdbc_execution_for_an_insert_input() {
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        String anInsertStatement = "INSERT INTO A_TABLE VALUES(1, 2, 3)";
        quickSqlTestData.generateInsertScriptFor(anInsertStatement);
    }

}
