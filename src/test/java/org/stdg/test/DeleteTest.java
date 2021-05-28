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

import static org.stdg.test.TestTable.TestTableAssert.assertThat;
import static org.stdg.test.TestTable.buildUniqueTable;

public class DeleteTest extends H2Config {

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

        String deleteQuery = "DELETE FROM " + table1.getTableName();

        // WHEN
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(deleteQuery);

        // THEN
        table1.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(table1).withScript(insertScript)
                          .hasNumberOfRows(2)
                          .row(0).hasValues(1, "A1", "B1", 1.80)
                          .row(1).hasValues(2, "A2", "B2", 2.99);
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

        String updateQuery = "DELETE FROM " + table1.getTableName()
                           + " WHERE Col_A = 'A1'";

        // WHEN
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(updateQuery);

        // THEN
        table1.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(table1).withScript(insertScript)
                          .hasNumberOfRows(1)
                          .row(0).hasValues(1, "A1", "B1", 1.80);

    }

}
