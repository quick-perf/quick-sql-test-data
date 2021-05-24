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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.stdg.DatasetRow;
import org.stdg.SqlTestDataGenerator;
import org.testcontainers.containers.OracleContainer;

import javax.sql.DataSource;
import java.util.List;

import static org.stdg.test.TestTable.*;
import static org.stdg.test.TestTable.TestTableAssert.assertThat;

public class OracleTest {

    private static DataSource DATA_SOURCE;

    private static final OracleContainer ORACLE_CONTAINER
            = new OracleContainer("gvenzl/oracle-xe")
             .withEnv("ORACLE_PASSWORD", "oracle");

    private static SqlExecutor SQL_EXECUTOR;

    @BeforeAll
    public static void beforeAll() {
        ORACLE_CONTAINER.start();
        String jdbcUrl = ORACLE_CONTAINER.getJdbcUrl();
        DATA_SOURCE = DataSourceBuilder.INSTANCE.build(jdbcUrl
                                                      , ORACLE_CONTAINER.getUsername()
                                                     , ORACLE_CONTAINER.getPassword());
        SQL_EXECUTOR = new SqlExecutor(DATA_SOURCE);
    }

    @AfterAll
    public static void stopContainer() {
        ORACLE_CONTAINER.stop();
    }

    @Test
    public void should_generate_working_insert() {

        // GIVEN
        TestTable table =
                buildUniqueTable(DATA_SOURCE
                                , "TABLE_NAME"
                                , "col1 NUMBER"
                                + ", col2 varchar(25)"
                                + ", col3 varchar(25)")
                .create()
                .insertValues("1, 'col2_val', 'col3_val'");

        String tableName = table.getTableName();
        DatasetRow datasetRow =
                DatasetRow.ofTable(tableName)
                        .addColumnValue("col1", 1)
                .addColumnValue("col2", "col2_val")
                .addColumnValue("col3", "col3_val");

        //WHEN
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);

        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(datasetRow);

        // THEN
        table.recreate();

        SQL_EXECUTOR.execute(insertStatements);
        assertThat(table)
                .withGeneratedInserts(insertStatements)
                .hasNumberOfRows(1)
                .row(0).hasValues(1, "col2_val", "col3_val");

    }

}
