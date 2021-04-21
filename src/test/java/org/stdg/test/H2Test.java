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
import org.stdg.SqlTestDataGenerator;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class H2Test extends H2Configuration {

    @Test public void
    should_generate_working_insert_from_a_select_statement() {

        // GIVEN
        TestTable playerTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Player"
                                          , "  id bigint"
                                          + ", firstName varchar(255)"
                                          + ", lastName varchar(255)"
                                          )
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
        assertThat(insertScript).contains("ID, FIRSTNAME, LASTNAME");

        playerTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(playerTable).hasNumberOfRows(1);

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

    @Test public void
    should_generate_an_insert_statement_with_not_null_columns_from_a_statement_selecting_a_null_column() {

        // GIVEN
        TestTable playerTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Player"
                                          , "  id bigint not null"
                                          + ", firstName varchar(255) not null"
                                          + ", lastName varchar(255) not null"
                                          + ", team_id bigint"
                                          )
                .create()
                .insertValues("1, 'Paul', 'Pogba', NULL");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT team_id FROM " + playerTableName + " WHERE lastName = 'Pogba'";
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

    @Test public void
    should_generate_an_insert_statement_with_not_null_columns_from_a_statement_selecting_2_columns() {

        // GIVEN
        TestTable playerTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Player"
                                          , "  id bigint not null"
                                          + ", firstName varchar(255) not null"
                                          + ", lastName varchar(255) not null"
                                          + ", team_id bigint"
                                          )
                .create()
                .insertValues("1, 'Paul', 'Pogba', NULL");

        // WHEN
        String playerTableName = playerTable.getTableName();
        String select = "SELECT id, team_id  FROM " + playerTableName + " WHERE lastName = 'Pogba'";
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

    @Test public void
    should_add_not_null_columns_to_rows_related_to_a_not_null_foreign_key() {

        // GIVEN
        TestTable teamTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Team"
                                          ," id bigint not null" +
                                          ",  name varchar(255) not null" +
                                          ",  primary key (id)")
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
        TestTable.TestTableAssert.assertThat(teamTable).row(0).column(0).hasValues(1);
        TestTable.TestTableAssert.assertThat(teamTable).row(0).column(1).hasValues("Manchester United");

    }

    @Test public void
    should_add_joined_rows_of_joined_rows_because_of_not_null_foreign_keys() {

        // GIVEN
        TestTable sponsorTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Sponsor"
                                          , "  id bigint" +
                                          ",  name varchar(255) not null" +
                                          ", primary key (id)"
                                          )
                        .create()
                        .insertValues("1, 'Sponsor name'");

        String teamSponsorForeignKey = "add constraint team_sponsor_fk" + generateRandomPositiveInt()
                                     + " foreign key (sponsor_id)"
                                     + " references " + sponsorTable.getTableName();

        TestTable teamTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Team"
                                          ," id bigint not null" +
                                          ", name varchar(255) not null" +
                                          ", sponsor_id bigint not null" +
                                          ", primary key (id)"
                                          )
                        .create()
                        .alter(teamSponsorForeignKey)
                        .insertValues("1, 'Manchester United', 1");

        String playerTeamForeignKey =  "add constraint player_team_fk" + generateRandomPositiveInt()
                + " foreign key (team_id)"
                + " references " + teamTable.getTableName();
        TestTable playerTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Player"
                                          , "id bigint not null"
                                          + ", firstName varchar(255)"
                                          + ", lastName varchar(255)"
                                          + ", team_id bigint not null"
                                          + ", primary key (id)")
                        .create()
                        .alter(playerTeamForeignKey)
                        .insertValues("1, 'Paul', 'Pogba', 1");

        // WHEN
        String playerSelect = "SELECT * FROM " + playerTable.getTableName();
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(playerSelect);

        // THEN
        playerTable.drop();
        teamTable.drop();
        sponsorTable.drop().create();
        teamTable.create().alter(teamSponsorForeignKey);
        playerTable.create().alter(playerTeamForeignKey);
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(playerTable).withScript(insertScript).hasNumberOfRows(1);
        TestTable.TestTableAssert.assertThat(teamTable).withScript(insertScript).hasNumberOfRows(1);
        TestTable.TestTableAssert.assertThat(teamTable).row(0).column(0).hasValues(1);
        TestTable.TestTableAssert.assertThat(teamTable).row(0).column(1).hasValues("Manchester United");
        TestTable.TestTableAssert.assertThat(sponsorTable).withScript(insertScript).hasNumberOfRows(1);

    }

}
