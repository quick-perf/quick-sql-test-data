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
import org.stdg.SqlTestDataGenerator;

import javax.sql.DataSource;

import static org.stdg.test.TestTable.buildUniqueTable;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class DeleteTest {

    private static DataSource DATA_SOURCE;

    private static SqlExecutor SQL_EXECUTOR;

    @BeforeAll
    public static void beforeAll() {
        DATA_SOURCE = DataSourceBuilder.build("jdbc:h2:mem:test", "user", "pwd");
        SQL_EXECUTOR = new SqlExecutor(DATA_SOURCE);
    }

    @Test
    public void
    should_generate_insert_if_all_rows_are_deleted_and_no_mandatory_columns() {

        // GIVEN
        TestTable table1 =
                buildUniqueTable(DATA_SOURCE
                        , "Table_1"
                        , "  id bigint"
                                + ", Col_A varchar(255)"
                                + ", Col_B varchar(255)"
                                + ", Col_dec decimal")
                        .create()
                        .insertValues("1, 'A1', 'B1', 1.80")
                        .insertValues("2, 'A2', 'B2', 2.99");

        // WHEN
        String table1Name = table1.getTableName();
        String deleteQuery = "DELETE FROM " + table1Name;

        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(deleteQuery);

        // THEN
        table1.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(table1).withScript(insertScript).hasNumberOfRows(2);
        TestTable.TestTableAssert.assertThat(table1).withScript(insertScript).row(0).hasValues(1, "A1", "B1", 1.80);
        TestTable.TestTableAssert.assertThat(table1).withScript(insertScript).row(1).hasValues(2, "A2", "B2", 2.99);
    }

    @Test
    public void
    should_generate_one_insert_if_one_rows_is_deleted() {

        // GIVEN
        TestTable table1 =
                buildUniqueTable(DATA_SOURCE
                        , "Table_1"
                        , "  id bigint"
                                + ", Col_A varchar(255)"
                                + ", Col_B varchar(255)"
                                + ", Col_dec decimal")
                        .create()
                        .insertValues("1, 'A1', 'B1', 1.80")
                        .insertValues("2, 'A2', 'B2', 2.99");

        // WHEN
        String table1Name = table1.getTableName();
        String updateQuery = "DELETE FROM " + table1Name
                + " WHERE Col_A = 'A1'";

        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(updateQuery);

        // THEN
        table1.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(table1).withScript(insertScript).hasNumberOfRows(1);
        TestTable.TestTableAssert.assertThat(table1).withScript(insertScript).row(0).hasValues(1, "A1", "B1", 1.80);
    }
}
