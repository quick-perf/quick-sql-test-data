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

import org.junit.jupiter.api.Test;
import org.stdg.SqlTestDataGenerator;

import java.util.Arrays;
import java.util.List;

import static org.stdg.test.TestTable.*;
import static org.stdg.test.TestTable.TestTableAssert.*;

public class SelectTest extends H2Config {

    @Test public void
    should_generate_working_insert_from_a_select_statement() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Player"
                                , "  id bigint"
                                + ", firstName varchar(255)"
                                + ", lastName varchar(255)"
                                )
                .create()
                .insertValues("1, 'Paul', 'Pogba'")
                .insertValues("2, 'Antoine', 'Griezmann'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(2)
                               .row(0).hasValues(1, "Paul", "Pogba")
                               .row(1).hasValues(2, "Antoine", "Griezmann");

    }

    @Test public void
    should_generate_an_insert_statement_from_a_select_containing_bind_parameters() {

        // GIVEN
        TestTable table =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "col1 varchar(25)"
                                + ", col2 varchar(25)"
                                + ", col3 varchar(25)"
                                )
                .create()
                .insertValues("'val1', 'val2', 'val3'");

        String tableName = table.getTableName();
        String select = " SELECT col1, col2, col3 FROM " + tableName
                      + " WHERE col2=? AND col3=?";

        List<Object> parameterValues = Arrays.asList("val2", "val3");

        // WHEN
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select, parameterValues);

        // THEN
        table.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(table).withScript(insertScript)
                         .hasNumberOfRows(1)
                         .row(0).hasValues("val1", "val2", "val3");

    }

}
