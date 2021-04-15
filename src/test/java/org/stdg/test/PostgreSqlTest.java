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
import org.stdg.SqlTestDataGenerator;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PostgreSqlTest {

    private static final String DB_USER_NAME = "user";

    private static final String DB_PASSWORD = "pwd";

    private static final PostgreSQLContainer POSTGRESQL_CONTAINER
            = new PostgreSQLContainer<>("postgres:12.3")
            .withDatabaseName("postgresql")
            .withUsername(DB_USER_NAME)
            .withPassword(DB_PASSWORD);

    private static DataSource DATA_SOURCE;

    private static SqlExecutor SQL_EXECUTOR;

    @BeforeAll
    public static void beforeAll() {
        POSTGRESQL_CONTAINER.start();
        String jdbcUrl = POSTGRESQL_CONTAINER.getJdbcUrl();
        DATA_SOURCE = DataSourceBuilder.INSTANCE.build(jdbcUrl, DB_USER_NAME, DB_PASSWORD);
        SQL_EXECUTOR = new SqlExecutor(DATA_SOURCE);
    }

    @AfterAll
    public static void stopContainer() {
        POSTGRESQL_CONTAINER.stop();
    }

    @Test public void
    should_generate_working_insert_from_a_select_statement() {

        // GIVEN
        TestTable playerTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Player"
                                          , "  id bigint"
                                          + ", firstName varchar(255)"
                                          + ", lastName varchar(255)")
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
        TestTable.TestTableAssert.assertThat(playerTable).withScript(insertScript).hasNumberOfRows(2);
        TestTable.TestTableAssert.assertThat(playerTable).row(0).column(0).hasValues(1, 2);
        TestTable.TestTableAssert.assertThat(playerTable).row(0).column(1).hasValues("Paul", "Antoine");
        TestTable.TestTableAssert.assertThat(playerTable).row(0).column(2).hasValues("Pogba", "Griezmann");

    }

    @Test public void
    should_generate_an_insert_statement_with_columns_declared_in_the_same_order_as_in_the_table() {

        // GIVEN
        TestTable playerTable =
                TestTable.buildUniqueTable(DATA_SOURCE
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
        assertThat(insertScript).contains("id, firstname, lastname");

        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(playerTable).withScript(insertScript).hasNumberOfRows(1);

    }

    @Test public void
    should_generate_an_insert_with_selected_columns_only() {

        // GIVEN
        TestTable playerTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Player"
                                          , "id bigint"
                                          + ", firstName varchar(255)"
                                          + ", lastName varchar(255)"
                                          )
                .create()
                .insertValues("1, 'Paul', 'Pogba'");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT firstName FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select);

        // THEN
        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(playerTable).withScript(insertScript).hasNumberOfRows(1);
        TestTable.TestTableAssert.assertThat(playerTable).row(0).column(1).hasValues("Paul");

    }

    @Test public void
    should_generate_an_insert_statement_with_not_null_columns() {

        // GIVEN
        TestTable playerTable =
                TestTable.buildUniqueTable(DATA_SOURCE
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
        TestTable.TestTableAssert.assertThat(playerTable).withScript(insertScript).hasNumberOfRows(1);
        TestTable.TestTableAssert.assertThat(playerTable).row(0).column(0).hasValues(1);
        TestTable.TestTableAssert.assertThat(playerTable).row(0).column(1).hasValues("Paul");
        TestTable.TestTableAssert.assertThat(playerTable).row(0).column(2).hasValues("Pogba");

    }

    @RepeatedTest(9) public void
    should_order_insert_queries_following_table_dependencies() {

        // GIVEN
        TestTable teamTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Team"
                                          ," id bigint not null" +
                                          ",  name varchar(255)" +
                                          ",  primary key (id)"
                                          )
                .create()
                .insertValues("1, 'Manchester United'");

        String playerTableConstraint = "add constraint player_team_fk" + generateRandomPositiveInt()
                                    + " foreign key (team_id)"
                                    + " references " + teamTable.getTableName();
        TestTable playerTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Player"
                                          , "id bigint not null"
                                          + ", firstName varchar(255)"
                                          + ", lastName varchar(255)"
                                          + ", team_id bigint"
                                          + ", primary key (id)"
                                          )
                .create()
                .alter(playerTableConstraint)
                .insertValues("1, 'Paul', 'Pogba', 1");

        // WHEN
        String playerSelect = "SELECT * FROM " + playerTable.getTableName();
        String teamSelect = "SELECT * FROM " + teamTable.getTableName();

        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator
                             .generateInsertScriptFor(playerSelect, teamSelect);

        // THEN
        playerTable.drop();
        teamTable.drop().create();
        playerTable.create().alter(playerTableConstraint);
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(playerTable).withScript(insertScript).hasNumberOfRows(1);
        TestTable.TestTableAssert.assertThat(teamTable).hasNumberOfRows(1);

    }

    private int generateRandomPositiveInt() {
        Random random = new Random();
        return Math.abs(random.nextInt());
    }

    @Test public void
    should_add_rows_related_to_a_not_null_foreign_key() {

        // GIVEN
        TestTable teamTable =
                        TestTable.buildUniqueTable(DATA_SOURCE
                                                  , "Team"
                                                  ," id bigint not null" +
                                                  ",  name varchar(255)" +
                                                  ",  primary key (id)"
                                                  )
                        .create()
                        .insertValues("1, 'Manchester United'");

        String playerTableConstraint = "add constraint player_team_fk" + generateRandomPositiveInt()
                                    + " foreign key (team_id)"
                                    + " references " + teamTable.getTableName();
        TestTable playerTable =
                        TestTable.buildUniqueTable(DATA_SOURCE
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
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(playerSelect);

        // THEN
        playerTable.drop();
        teamTable.drop().create();
        playerTable.create().alter(playerTableConstraint);
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(playerTable).withScript(insertScript).hasNumberOfRows(1);
        TestTable.TestTableAssert.assertThat(teamTable).hasNumberOfRows(1);

    }

    @RepeatedTest(9) public void
    should_order_insert_following_primary_keys() {

        // GIVEN
        TestTable table =
                TestTable.buildUniqueTable(DATA_SOURCE
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

}
