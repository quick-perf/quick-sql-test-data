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

package org.stdg.test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.quickperf.junit5.QuickPerfTest;
import org.quickperf.sql.annotation.ExpectJdbcQueryExecution;
import org.stdg.SqlTestDataGenerator;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.quickperf.sql.config.QuickPerfSqlDataSourceBuilder.aDataSourceBuilder;

@QuickPerfTest
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class InsertTest {

    static DataSource DATA_SOURCE;

    static SqlExecutor SQL_EXECUTOR;

    @BeforeAll
    public static void beforeAll() {
        DataSource h2Datasource = DataSourceBuilder.build("jdbc:h2:mem:test", "user", "pwd");
        DATA_SOURCE = aDataSourceBuilder().buildProxy(h2Datasource);
        SQL_EXECUTOR = new SqlExecutor(DATA_SOURCE);
    }

    @Test public void
    should_generate_an_empty_insert_script_for_an_insert_input() {
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String anInsertStatement = "INSERT INTO A_TABLE VALUES(1, 2, 3)";
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(anInsertStatement);
        assertThat(insertScript).isEmpty();
    }

    @Test @ExpectJdbcQueryExecution(0) public void
    should_not_use_jdbc_execution_for_an_insert_input() {
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String anInsertStatement = "INSERT INTO A_TABLE VALUES(1, 2, 3)";
        sqlTestDataGenerator.generateInsertScriptFor(anInsertStatement);
    }

}
