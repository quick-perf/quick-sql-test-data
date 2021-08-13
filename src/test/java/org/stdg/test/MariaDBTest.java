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

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.stdg.SqlTestDataGenerator;
import org.testcontainers.containers.MariaDBContainer;

import javax.sql.DataSource;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.stdg.test.TestTable.TestTableAssert.assertThat;
import static org.stdg.test.TestTable.buildUniqueTable;

public class MariaDBTest {

    private static final String DB_USER_NAME = "user";

    private static final String DB_PASSWORD = "pwd";

    private static final MariaDBContainer MARIA_DB_CONTAINER
            = new MariaDBContainer<>("mariadb:10.5.2")
            .withDatabaseName("mariadb")
            .withUsername(DB_USER_NAME)
            .withPassword(DB_PASSWORD);

    private static DataSource DATA_SOURCE;

    private static SqlExecutor SQL_EXECUTOR;

    @BeforeAll
    public static void beforeAll() {
        MARIA_DB_CONTAINER.start();
        String jdbcUrl = MARIA_DB_CONTAINER.getJdbcUrl();
        DATA_SOURCE = DataSourceBuilder.INSTANCE.build(jdbcUrl, DB_USER_NAME, DB_PASSWORD);
        SQL_EXECUTOR = new SqlExecutor(DATA_SOURCE);
    }

    @AfterAll
    public static void stopContainer() {
        MARIA_DB_CONTAINER.stop();
    }

    @Test public void
    should_generate_working_insert_from_a_select_statement() {

        // GIVEN
        String colDescsAndConstraints = "  id bigint"
                                      + ", firstName varchar(255)"
                                      + ", lastName varchar(255)";
        TestTable playerTable = buildUniqueTable(DATA_SOURCE
                                                , "Player"
                                                , colDescsAndConstraints);
        playerTable.create()
                   .insertValues("1, 'Paul', 'Pogba'");

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
                               .row(0).hasValues(1, "Paul", "Pogba");

    }

    @Test public void
    should_generate_an_insert_statement_with_columns_declared_in_the_same_order_as_in_the_table() {

        // GIVEN
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "player"
                                , "id bigint"
                                + ", firstName varchar(255)"
                                + ", lastName varchar(255)"
                                )
                .create()
                .insertValues("1, 'Paul', 'Pogba'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT * FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        assertThat(insertScript).contains("id, firstName, lastName");

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
                                + ", lastName varchar(255) not null"
                                )
                .create()
                .insertValues("1, 'Paul', 'Pogba'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT id FROM " + playerTableName + " WHERE lastName = 'Pogba'";
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1)
                               .row(0).hasValues(1, "Paul", "Pogba");

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
                                ",  primary key (id)"
                                )
                        .create()
                        .insertValues("1, 'Manchester United'");

        String playerTableConstraint = "add constraint player_team_fk" + generateRandomPositiveInt()
                                     + " foreign key (team_id)"
                                     + " references " + teamTable.getTableName() + " (id)";
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
                                , "Player"
                                , "id bigint not null"
                                + ", firstName varchar(255)"
                                + ", lastName varchar(255)"
                                + ", team_id bigint not null"
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
        assertThat(firstInsert).as(insertStatementsAsString).contains("VALUES(1, 1");

        String secondInsert = insertStatements.get(1);
        assertThat(secondInsert).as(insertStatementsAsString).contains("VALUES(1, 2");

        String thirdInsert = insertStatements.get(2);
        assertThat(thirdInsert).as(insertStatementsAsString).contains("VALUES(2, 1");

        String fourthInsert = insertStatements.get(3);
        assertThat(fourthInsert).as(insertStatementsAsString).contains("VALUES(2, 2");

    }

    // Not possible to both repeat and parameterize a JUnit 5 test
    @ParameterizedTest
    @ValueSource(strings = {"INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT"})
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
        assertThat(firstQuery).as(insertStatementsAsString).contains("VALUES(1");

        String secondQuery = insertStatements.get(1);
        assertThat(secondQuery).as(insertStatementsAsString).contains("VALUES(2");

        String thirdQuery = insertStatements.get(2);
        assertThat(thirdQuery).as(insertStatementsAsString).contains("VALUES(10");

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
        String insertQueriesAsString = insertStatements.toString();

        String firstInsert = insertStatements.get(0);
        assertThat(firstInsert).as(insertQueriesAsString).contains("VALUES(1, 1");

        String secondInsert = insertStatements.get(1);
        assertThat(secondInsert).as(insertQueriesAsString).contains("VALUES(2, 1");

        String thirdInsert = insertStatements.get(2);
        assertThat(thirdInsert).as(insertQueriesAsString).contains("VALUES(1, 2");

        String fourthInsert = insertStatements.get(3);
        assertThat(fourthInsert).as(insertQueriesAsString).contains("VALUES(2, 2");

    }

    // Currently there is no "timestamp with timezone" ( https://jira.mariadb.org/browse/MDEV-10018 )

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
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        assertThat(playerTable).withScript(insertScript)
                               .hasNumberOfRows(1);
        assertThat(insertScript).contains("'2012-09-17 19:56:47.0'");
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