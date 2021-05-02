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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.stdg.DatabaseMetadataFinder;
import org.stdg.SqlTestDataGenerator;
import org.stdg.dbtype.DatabaseType;

import static org.stdg.dbtype.DatabaseMetadataFinderFactory.*;
import static org.stdg.test.TestTable.TestTableAssert.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class NotFullyManagedDatabaseTest extends H2Configuration {

    @Test public void
    should_generate_insert_statements_without_necessarily_taking_account_of_database_constraints_if_the_database_is_not_supposed_to_be_fully_managed() {

        // GIVEN
        TestTable table =
                TestTable.buildUniqueTable(DATA_SOURCE
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
        SqlTestDataGenerator sqlTestDataGeneratorOfNotFullyManagedDatabase =
                SqlTestDataGenerator.buildFrom(DATA_SOURCE, databaseMetadataFinderOfNotFullyManagedDatabase);

        String playerTableName = table.getTableName();
        String select = "SELECT col1, col2 FROM " + playerTableName;

        // WHEN
        String insertScript = sqlTestDataGeneratorOfNotFullyManagedDatabase.generateInsertScriptFor(select);

        // THEN
        table.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(table).hasNumberOfRows(2);

    }

}
