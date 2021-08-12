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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.stdg.SqlTestDataGenerator;
import org.testcontainers.containers.MSSQLServerContainer;

import javax.sql.DataSource;
import java.util.List;
import java.util.Random;

import static org.stdg.test.TestTable.TestTableAssert.assertThat;
import static org.stdg.test.TestTable.buildUniqueTable;

public class MSSQLServerTest {

    private static final MSSQLServerContainer MS_SQL_SERVER
            = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2019-CU9-ubuntu-16.04")
              .acceptLicense();

    private static DataSource DATA_SOURCE;

    private static SqlExecutor SQL_EXECUTOR;

    @BeforeAll
    public static void beforeAll() {
        MS_SQL_SERVER.start();
        String jdbcUrl = MS_SQL_SERVER.getJdbcUrl();
        String userName = MS_SQL_SERVER.getUsername();
        String password = MS_SQL_SERVER.getPassword();
        DATA_SOURCE = DataSourceBuilder.INSTANCE.build(jdbcUrl, userName, password);
        SQL_EXECUTOR = new SqlExecutor(DATA_SOURCE);
    }

    @AfterAll
    public static void stopContainer() {
        MS_SQL_SERVER.stop();
    }

    @Test public void
    should_generate_working_insert_from_a_select_statement() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Player"
                                , "id bigint not null, firstName varchar(255)"
                                + ", lastName varchar(255), primary key (id)")
                .create()
                .insertValues("1, 'Paul', 'Pogba'")
                .insertValues("2, 'Antoine', 'Griezmann'");

        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;

        // WHEN
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(2);

    }


    @Test public void
    should_generate_an_insert_statement_with_columns_declared_in_the_same_order_as_in_the_table() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "player"
                                , "id bigint"
                                + ", firstName varchar(255)"
                                + ", lastName varchar(255)")
                .create()
                .insertValues("1, 'Paul', 'Pogba'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        Assertions.assertThat(insertScript).contains("id, firstName, lastName");

        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1);

    }

    @Test public void
    should_generate_an_insert_statement_with_not_null_columns() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Player"
                                , "  id bigint not null"
                                + ", firstName varchar(255) not null"
                                + ", lastName varchar(255) not null")
               .create()
               .insertValues("1, 'Paul', 'Pogba'");

        String playerTableName = playerTable.getTableName();
        String select = "SELECT id FROM " + playerTableName + " WHERE lastName = 'Pogba'";

        // WHEN
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1)
                               .row(0).hasValues(1, "Paul", "Pogba");

    }

    @RepeatedTest(9) public void
    should_sort_insert_statements_following_table_dependencies() {

        // GIVEN
        TestTable teamTable =
                buildUniqueTable(DATA_SOURCE
                                , "Team"
                                ," id bigint not null"
                                + ",  name varchar(255)"
                                + ",  primary key (id)")
                        .create()
                        .insertValues("1, 'Manchester United'");

        String playerTableConstraint = "add constraint FKqfn7q18rx1dwkwui2tyl30e08" + generateRandomPositiveInt()
                                     + " foreign key (team_id)"
                                     + " references " + teamTable.getTableName();
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Player"
                                , "id bigint not null"
                                + ", firstName varchar(255)"
                                + ", lastName varchar(255)"
                                + ", team_id bigint"
                                + ", primary key (id)")
                        .create()
                        .alter(playerTableConstraint)
                        .insertValues("1, 'Paul', 'Pogba', 1");

        String playerSelect = "SELECT * FROM " + playerTable.getTableName();
        String teamSelect = "SELECT * FROM " + teamTable.getTableName();

        // WHEN
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator
                             .generateInsertScriptFor(playerSelect, teamSelect);

        // THEN
        playerTable.drop();
        teamTable.drop().create();
        playerTable.create().alter(playerTableConstraint);
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1);
        assertThat(teamTable).withScript(insertScript)
                             .hasNumberOfRows(1);

    }

    private int generateRandomPositiveInt() {
        Random random = new Random();
        return Math.abs(random.nextInt());
    }

    @Test public void
    should_add_rows_related_to_a_not_null_foreign_key() {

        // GIVEN
        TestTable teamTable =
                buildUniqueTable(DATA_SOURCE
                                , "Team"
                                ," id bigint not null" +
                                ",  name varchar(255)" +
                                ",  primary key (id)")
                        .create()
                        .insertValues("1, 'Manchester United'");

        String playerTableConstraint = "add constraint player_team_fk" + generateRandomPositiveInt()
                                      + " foreign key (team_id)"
                                      + " references " + teamTable.getTableName();
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Player"
                                , "id bigint not null"
                                + ", firstName varchar(255)"
                                + ", lastName varchar(255)"
                                + ", team_id bigint not null"
                                + ", primary key (id)")
                        .create()
                        .alter(playerTableConstraint)
                        .insertValues("1, 'Paul', 'Pogba', 1");

        // WHEN
        String playerSelect = "SELECT * FROM " + playerTable.getTableName();
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(playerSelect);

        // THEN
        playerTable.drop();
        teamTable.drop().create();
        playerTable.create().alter(playerTableConstraint);
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript).hasNumberOfRows(1);
        assertThat(teamTable).hasNumberOfRows(1);

    }

    @RepeatedTest(9) public void
    should_sort_insert_statements_following_the_primary_key_values() {

        // GIVEN
        TestTable table =
                buildUniqueTable(DATA_SOURCE
                                , "comp_pk"
                                , "col_id1   integer," +
                                "col_id2   integer, " +
                                "colA  varchar(20), " +
                                "colB  varchar(20), " +
                                "constraint comp_pk_pk" + generateRandomPositiveInt() + " primary key (col_id1, col_id2)"
                                )
                        .create()
                        .insertValues("1, 2, 'colA_r1_value', 'colB_r1_value'")
                        .insertValues("1, 1, 'colA_r1_value', 'colB_r1_value'")
                        .insertValues("2, 2, 'colA_r1_value', 'colB_r1_value'")
                        .insertValues("2, 1, 'colA_r1_value', 'colB_r1_value'");

        // WHEN
        String select1 = "SELECT * FROM " + table.getTableName() + " WHERE col_id1 = 1";
        String select2 = "SELECT * FROM " + table.getTableName() + " WHERE col_id1 = 2";
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(select1, select2);

        // THEN
        String insertStatementsAsString = insertStatements.toString();

        String firstInsert = insertStatements.get(0);
        Assertions.assertThat(firstInsert).as(insertStatementsAsString).contains("VALUES(1, 1");

        String secondInsert = insertStatements.get(1);
        Assertions.assertThat(secondInsert).as(insertStatementsAsString).contains("VALUES(1, 2");

        String thirdInsert = insertStatements.get(2);
        Assertions.assertThat(thirdInsert).as(insertStatementsAsString).contains("VALUES(2, 1");

        String fourthInsert = insertStatements.get(3);
        Assertions.assertThat(fourthInsert).as(insertStatementsAsString).contains("VALUES(2, 2");

    }

    @RepeatedTest(9) public void
    should_sort_insert_statements_following_the_primary_key_values_with_a_composite_composite_primary_key_having_columns_not_in_same_order_as_in_table_declaration() {

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
        String insertStatementsAsString = insertStatements.toString();

        String firstInsert = insertStatements.get(0);
        Assertions.assertThat(firstInsert).as(insertStatementsAsString).contains("VALUES(1, 1");

        String secondInsert = insertStatements.get(1);
        Assertions.assertThat(secondInsert).as(insertStatementsAsString).contains("VALUES(2, 1");

        String thirdInsert = insertStatements.get(2);
        Assertions.assertThat(thirdInsert).as(insertStatementsAsString).contains("VALUES(1, 2");

        String fourthInsert = insertStatements.get(3);
        Assertions.assertThat(fourthInsert).as(insertStatementsAsString).contains("VALUES(2, 2");

    }

    // Not possible to both repeat and parameterize a JUnit 5 test
    @ParameterizedTest
    @ValueSource(strings = {"INT", "SMALLINT", "TINYINT", "BIGINT"})
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
                        , "date Date"
                )
                        .create()
                        .insertValues("'2012-09-17'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                .hasNumberOfRows(1)
                .row(0).hasValues("2012-09-17");

    }

    @Test public void
    should_generate_an_insert_statement_with_a_timestamp_type() {
        // DATETIME2 is a timestamp type an accuracy of 100 nanoseconds
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                        , "Table"
                        , "timestampCol DATETIME2"
                        )
                .create()
                .insertValues("'2012-09-17 19:56:47.32'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                .hasNumberOfRows(1);
        Assertions.assertThat(insertScript).contains("'2012-09-17 19:56:47.32'");
    }

    @Test public void
    should_generate_an_insert_statement_with_a_smalldatetime_type() {
        // SMALLDATETIME is a timestamp type an accuracy of 1 minute
        TestTable playerTable =
            buildUniqueTable(DATA_SOURCE
                , "Table"
                , "timestampCol SMALLDATETIME"
            )
                .create()
                .insertValues("'2012-09-17 19:56:47.32'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1);
        Assertions.assertThat(insertScript).contains("'2012-09-17 19:57:00.0'");
    }


    @Test public void
    should_generate_an_insert_statement_with_a_timestamp_with_time_zone_type() {
        /*
        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                        , "Table"
                        , "col DATETIMEOFFSET"
                        )
               .create()
               .insertValues("'2020-12-20 17:20:13 +03:00'");
//               .insertValues("'20131114 08:54:00 +10:00'");
//               .insertValues("SYSDATETIMEOFFSET()");
//             .insertValues("TODATETIMEOFFSET(SYSDATETIME(), '+13:00')");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN

        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1);
        */

//        java.lang.IllegalStateException: Unable to execute
//        INSERT INTO Table_779310794(col) VALUES(2021-08-12 22:34:43.8784766 +13:00);

//        Caused by: com.microsoft.sqlserver.jdbc.SQLServerException: Incorrect syntax near '17'.
//            at com.microsoft.sqlserver.jdbc.SQLServerException.makeFromDatabaseError(SQLServerException.java:262)

    }

    @Test public void
    should_generate_an_insert_statement_with_a_time_with_timezone_type() {
        // MSL SQL server has no time with time zone
        // https://docs.microsoft.com/en-gb/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15
        // a DateTimeOffset is cast as time type

        // GIVEN
        TestTable playerTable =
            buildUniqueTable(DATA_SOURCE
                , "Table"
                , "col TIME"
                )
            .create()
            .insertValues("cast('2020-12-20 17:20:13 +03:00' AS time)");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

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
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                .hasNumberOfRows(1)
                .row(0).hasValues("23:59:59");

    }

}
