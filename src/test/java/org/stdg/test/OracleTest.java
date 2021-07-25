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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.OracleContainer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.stdg.DatasetRow;
import org.stdg.SqlTestDataGenerator;

import javax.sql.DataSource;
import java.util.List;
import java.util.Random;

import static org.stdg.test.TestTable.TestTableAssert.assertThat;
import static org.stdg.test.TestTable.buildUniqueTable;

public class OracleTest {

    private static DataSource DATA_SOURCE;

    private static final OracleContainer ORACLE_CONTAINER
            = new OracleContainer("gvenzl/oracle-xe:18-slim")
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
        assertThat(table).withGeneratedInserts(insertStatements)
                         .hasNumberOfRows(1)
                         .row(0).hasValues(1, "col2_val", "col3_val");

    }

    @Test public void
    should_generate_an_insert_statement_with_columns_declared_in_the_same_order_as_in_the_table() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                 , "player"
                                 , "id number"
                                 + ", firstName varchar(255)"
                                 + ", lastName varchar(255)")
                .create()
                .insertValues("1, 'Paul', 'Pogba'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        String insertStatement = insertStatements.get(0);
        Assertions.assertThat(insertStatement).contains("ID, FIRSTNAME, LASTNAME");

        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);

    }

    @Test public void
    should_generate_an_insert_statement_with_not_null_columns() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Player"
                                , "  id number not null"
                                + ", firstName varchar(255) not null"
                                + ", lastName varchar(255) not null"
                                )
                .create()
                .insertValues("1, 'Paul', 'Pogba'");

        String select = "SELECT id FROM " + playerTable.getTableName()
                      + " WHERE lastName = 'Pogba'";

        // WHEN
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);


        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                .hasNumberOfRows(1)
                .row(0).hasValues(1,"Paul", "Pogba");

    }

    @RepeatedTest(9) public void
    should_sort_insert_statements_following_table_dependencies() {

        // GIVEN
        TestTable teamTable =
                buildUniqueTable(DATA_SOURCE
                                          , "Team"
                                          ," id number" +
                                          ",  name varchar(255)" +
                                          ",  primary key (id)"
                                          )
                .create()
                .insertValues("1, 'Manchester United'");

        String playerTableConstraint = "add constraint player_team_fk" + generateRandomPositiveInt()
                                     + " foreign key (team_id)"
                                     + " references " + teamTable.getTableName();
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                          , "Player"
                                          , "id number"
                                          + ", firstName varchar(255)"
                                          + ", lastName varchar(255)"
                                          + ", team_id number"
                                          + ", primary key (id)"
                                          )
                .create()
                .alter(playerTableConstraint)
                .insertValues("1, 'Paul', 'Pogba', 1");

        String playerSelect = "SELECT * FROM " + playerTable.getTableName();
        String teamSelect = "SELECT * FROM " + teamTable.getTableName();

        // WHEN
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(playerSelect, teamSelect);

        // THEN
        playerTable.drop();
        teamTable.drop().create();
        playerTable.create().alter(playerTableConstraint);
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);
        assertThat(teamTable).withGeneratedInserts(insertStatements)
                             .hasNumberOfRows(1);

    }

    @Test public void
    should_add_rows_related_to_a_not_null_foreign_key() {

        // GIVEN
        TestTable teamTable =
                buildUniqueTable(DATA_SOURCE
                        , "Team"
                        , " id number not null" +
                                ",  name varchar(255)" +
                                ",  primary key (id)"
                )
                        .create()
                        .insertValues("1, 'Manchester United'");

        String playerTableConstraint = "add constraint player_team_fk" + generateRandomPositiveInt()
                + " foreign key (team_id)"
                + " references " + teamTable.getTableName();
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                        , "Player"
                        , "id number not null"
                                + ", firstName varchar(255)"
                                + ", lastName varchar(255)"
                                + ", team_id number not null"
                                + ", primary key (id)"
                )
                        .create()
                        .alter(playerTableConstraint)
                        .insertValues("1, 'Paul', 'Pogba', 1");

        // WHEN
        String playerSelect = "SELECT * FROM " + playerTable.getTableName();
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(playerSelect);

        // THEN
        playerTable.drop();
        teamTable.drop().create();
        playerTable.create().alter(playerTableConstraint);
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);
        assertThat(teamTable).withGeneratedInserts(insertStatements)
                             .hasNumberOfRows(1);
    }

    @RepeatedTest(9) public void
    should_sort_insert_statements_following_primary_keys() {

        // GIVEN
        TestTable table =
               buildUniqueTable(DATA_SOURCE
                               , "comp_pk"
                               , "col_id1   integer," +
                                "col_id2   integer, " +
                                "colA  varchar(20), " +
                                "colB  varchar(20), " +
                                "constraint comp_pk_pk" + generateRandomPositiveInt() + " primary key (col_id2, col_id1)"
                               )
               .create()
               .insertValues("1, 2, 'colA_r1_value', 'colB_r1_value'")
               .insertValues("1, 1, 'colA_r1_value', 'colB_r1_value'")
               .insertValues("2, 2, 'colA_r1_value', 'colB_r1_value'")
               .insertValues("2, 1, 'colA_r1_value', 'colB_r1_value'");

        // WHEN
        String selectAll = "SELECT * FROM " + table.getTableName();
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(selectAll);

        // THEN
        String insertQueriesAsString = insertStatements.toString();

        String firstInsert = insertStatements.get(0);
        Assertions.assertThat(firstInsert).as(insertQueriesAsString).contains("VALUES(1, 1");

        String secondInsert = insertStatements.get(1);
        Assertions.assertThat(secondInsert).as(insertQueriesAsString).contains("VALUES(2, 1");

        String thirdInsert = insertStatements.get(2);
        Assertions.assertThat(thirdInsert).as(insertQueriesAsString).contains("VALUES(1, 2");

        String fourthInsert = insertStatements.get(3);
        Assertions.assertThat(fourthInsert).as(insertQueriesAsString).contains("VALUES(2, 2");

    }

    // Not possible to both repeat and parameterize a JUnit 5 test
    @ParameterizedTest
    @ValueSource(strings = {"INTEGER", "SMALLINT"})
    public void
    should_sort_insert_statements_following_an_integer_primary_key(String intType) {

        TestTable table =
                buildUniqueTable(DATA_SOURCE
                                , "table_with_int_pk"
                                , "col_id " + intType + "," +
                                "colA  varchar(20), " +
                                "colB  varchar(20), " +
                                "constraint int_pk" + generateRandomPositiveInt() + " primary key (col_id)"
                                )
                .create()
                .insertValues("2, 'A', 'B'")
                .insertValues("10, 'C', 'D'")
                .insertValues("1, 'E', 'F'");

        String selectAll = "SELECT * FROM " + table.getTableName();
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);

        // WHEN
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(selectAll);

        // THEN
        String insertStatementsAsString = insertStatements.toString();

        String firstQuery = insertStatements.get(0);
        Assertions.assertThat(firstQuery).as(insertStatementsAsString).contains("VALUES(1");

        String secondQuery = insertStatements.get(1);
        Assertions.assertThat(secondQuery).as(insertStatementsAsString).contains("VALUES(2");

        String thirdQuery = insertStatements.get(2);
        Assertions.assertThat(thirdQuery).as(insertStatementsAsString).contains("VALUES(10");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_date_type() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "dateCol Date"
                                )
                .create()
                .insertValues("TO_DATE('2012-09-17', 'yyyy-mm-dd')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1)
                               .row(0).hasValues("2012-09-17");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_date_type_with_day_number_less_than_10() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "dateCol Date"
                                )
                .create()
                .insertValues("TO_DATE('2012-09-05', 'yyyy-mm-dd')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                .hasNumberOfRows(1)
                .row(0).hasValues("2012-09-05");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_date_type_with_hour_less_than_10() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "dateCol Date"
                                )
                .create()
                .insertValues("TO_DATE('2012-09-05 08', 'yyyy-mm-dd HH24')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);

        String insertStatement = insertStatements.get(0);
        Assertions.assertThat(insertStatement).contains("8");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_date_type_with_hour_greater_than_10() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "dateCol Date"
                                )
                .create()
                .insertValues("TO_DATE('2012-09-05 13', 'yyyy-mm-dd HH24')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);

        String insertStatement = insertStatements.get(0);
        Assertions.assertThat(insertStatement).contains("13");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_date_type_and_a_month_after_september() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "dateCol Date"
                                )
                .create()
                .insertValues("TO_DATE('2012-10-17', 'yyyy-mm-dd')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1)
                               .row(0).hasValues("2012-10-17");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_date_type_and_minutes_less_than_10() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "dateCol Date"
                                )
                .create()
                .insertValues("TO_DATE('2012-09-17 16:09', 'yyyy-mm-dd HH24:MI')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);

        String insertStatement = insertStatements.get(0);
        Assertions.assertThat(insertStatement)
                  .as("Generated INSERT: " + insertStatement)
                  .contains("9");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_date_type_and_minutes_greater_than_10() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "dateCol Date"
                                )
                .create()
                .insertValues("TO_DATE('2012-09-17 16:28', 'yyyy-mm-dd HH24:MI')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);

        String insertStatement = insertStatements.get(0);
        Assertions.assertThat(insertStatement)
                  .as("Generated INSERT: " + insertStatement)
                  .contains("28");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_date_type_and_seconds_greater_than_nine() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "dateCol Date"
                                )
                .create()
                .insertValues("TO_DATE('2012-10-17 17:22:33', 'yyyy-mm-dd HH24:MI:SS')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);

    }

    @Test public void
    should_generate_an_insert_statement_with_a_timestamp_type_and_ms_less_than_100() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "timestampCol TIMESTAMP"
                                )
                .create()
                .insertValues("TO_TIMESTAMP('2012-09-17 19:56:47.21', 'YYYY-MM-DD HH24:MI:SS.FF')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);
        String insertStatement = insertStatements.get(0);
        Assertions.assertThat(insertStatement).contains("'2012-09-17 19:56:47.210'");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_timestamp_type_and_milliseconds_greater_than_100() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Table"
                                , "timestampCol TIMESTAMP"
                                )
                .create()
                .insertValues("TO_TIMESTAMP('2012-09-17 19:56:47.104', 'YYYY-MM-DD HH24:MI:SS.FF')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements)
                               .hasNumberOfRows(1);
        String insertStatement = insertStatements.get(0);
        Assertions.assertThat(insertStatement).contains("'2012-09-17 19:56:47.104'");

    }

    private int generateRandomPositiveInt() {
        Random random = new Random();
        return Math.abs(random.nextInt());
    }

}
