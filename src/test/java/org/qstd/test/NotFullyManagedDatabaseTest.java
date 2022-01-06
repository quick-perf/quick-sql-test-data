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

import org.junit.jupiter.api.Test;
import org.qstd.DatabaseMetadataFinder;
import org.qstd.QuickSqlTestData;
import org.qstd.dbtype.DatabaseType;

import static org.qstd.dbtype.DatabaseMetadataFinderFactory.createDatabaseMetadataFinderFrom;
import static org.qstd.test.TestTable.*;
import static org.qstd.test.TestTable.TestTableAssert.assertThat;

public class NotFullyManagedDatabaseTest extends H2Config {

    @Test public void
    should_generate_insert_statements_without_necessarily_taking_account_of_database_constraints_if_the_database_is_not_supposed_to_be_fully_managed() {

        // GIVEN
        TestTable table =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "col1 varchar(25)"
                                + ", col2 varchar(25)"
                                + ", col3 varchar(25)"
                                )
                .create()
                .insertValues("'val1', 'val2', 'val3'")
                .insertValues("'val3', 'val4', 'val5'");

        DatabaseMetadataFinder databaseMetadataFinderOfNotFullyManagedDatabase =
                createDatabaseMetadataFinderFrom(DATA_SOURCE, DatabaseType.OTHER);
        QuickSqlTestData quickSqlTestDataOfNotFullyManagedDatabase =
                QuickSqlTestData.buildFrom(DATA_SOURCE, DatabaseType.OTHER
                                             , databaseMetadataFinderOfNotFullyManagedDatabase);

        String select = "SELECT col1, col2 FROM " + table.getTableName();

        // WHEN
        String insertScript = quickSqlTestDataOfNotFullyManagedDatabase.generateInsertScriptFor(select);

        // THEN
        table.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(table).withScript(insertScript)
                         .hasNumberOfRows(2);

    }

}
