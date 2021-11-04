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
import org.qstd.DatasetRow;
import org.qstd.QuickSqlTestData;

import java.util.List;

import static org.qstd.test.TestTable.TestTableAssert.assertThat;
import static org.qstd.test.TestTable.buildUniqueTable;

public class DatasetRowApiTest extends H2Config {

    @Test public void
    should_generate_working_insert_from_a_dataset_row() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Player"
                                , "  id bigint"
                                + ", firstName varchar(255)"
                                + ", lastName varchar(255)")
                .create()
                .insertValues("1, 'Paul', 'Pogba'");

        // WHEN
        DatasetRow datasetRow =
                DatasetRow.ofTable(playerTable.getTableName())
                .addColumnValue("id", 1)
                .addColumnValue("firstName", "Paul")
                .addColumnValue("lastName", "Pogba");

        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        List<String> insertStatements = quickSqlTestData.generateInsertListFor(datasetRow);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1)
                               .row(0).hasValues(1, "Paul", "Pogba");

    }

}
