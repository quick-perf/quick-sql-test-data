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
 * Copyright 2021-2022 the original author or authors.
 */
package org.qstd.test;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.qstd.QuickSqlTestData;

import static org.qstd.test.TestTable.TestTableAssert.assertThat;
import static org.qstd.test.TestTable.buildUniqueTable;

public class H2DateTypesTest extends H2Config {

    @Test public void
    should_generate_an_insert_statement_with_a_date_type() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "date Date"
                                )
                .create()
                .insertValues("'2012-09-17'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        String insertScript = quickSqlTestData.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1)
                               .row(0).hasValues("2012-09-17");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_timestamp_type() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "timestampCol TIMESTAMP"
                                )
                .create()
                .insertValues("'2012-09-17 19:56:47.32'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        String insertScript = quickSqlTestData.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1);
        Assertions.assertThat(insertScript).contains("'2012-09-17 19:56:47.32'");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_timestamp_with_time_zone_type() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "col TIMESTAMP WITH TIME ZONE"
                                )
                .create()
                .insertValues("'2012-09-17 19:56:47.32 UTC'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        String insertScript = quickSqlTestData.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1);

    }

    @Test public void
    should_generate_an_insert_statement_with_a_time_type() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "col TIME"
                                )
                .create()
                .insertValues("'23:59:59'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        String insertScript = quickSqlTestData.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1)
                               .row(0).hasValues("23:59:59");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_time_with_timezone_type() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "col TIME WITH TIME ZONE"
                                )
                .create()
                .insertValues("'23:59:59 UTC'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        String insertScript = quickSqlTestData.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1);

    }

}
