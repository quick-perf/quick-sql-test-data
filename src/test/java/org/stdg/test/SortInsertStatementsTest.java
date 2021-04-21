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
import org.junit.jupiter.api.RepeatedTest;
import org.stdg.SqlTestDataGenerator;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.stdg.test.TestTable.TestTableAssert;
import static org.stdg.test.TestTable.buildUniqueTable;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class SortInsertStatementsTest extends H2Configuration {

    @RepeatedTest(9) public void
    should_sort_insert_statements_following_table_dependencies() {

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
                                     + " references " + teamTable.getTableName();
        TestTable playerTable =
                buildUniqueTable(DATA_SOURCE
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
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(playerSelect, teamSelect);

        // THEN
        playerTable.drop();
        teamTable.drop().create();
        playerTable.create().alter(playerTableConstraint);
        SQL_EXECUTOR.execute(insertScript);
        TestTableAssert.assertThat(playerTable).withScript(insertScript).hasNumberOfRows(1);
        TestTableAssert.assertThat(teamTable).hasNumberOfRows(1);

    }

    private int generateRandomPositiveInt() {
        Random random = new Random();
        return Math.abs(random.nextInt());
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

        String firstQuery = insertStatements.get(0);
        assertThat(firstQuery).as(insertStatementsAsString).contains("VALUES(1, 1");

        String secondQuery = insertStatements.get(1);
        assertThat(secondQuery).as(insertStatementsAsString).contains("VALUES(2, 1");

        String thirdQuery = insertStatements.get(2);
        assertThat(thirdQuery).as(insertStatementsAsString).contains("VALUES(1, 2");

        String fourthQuery = insertStatements.get(3);
        assertThat(fourthQuery).as(insertStatementsAsString).contains("VALUES(2, 2");

    }

    @RepeatedTest(9) public void
    should_sort_insert_statements_following_table_names_if_independent_tables() {

        TestTable testTable1 =
                buildUniqueTable(DATA_SOURCE
                                , "TABLE_1"
                                , "col varchar(20)"
                                )
                .create()
                .insertValues("'value_col_tab1'");

        TestTable testTable2 =
                buildUniqueTable(DATA_SOURCE
                                , "TABLE_2"
                                , "col varchar(20)"
                                )
                .create()
                .insertValues("'value_col_tab2'");

        String tab1Select = "SELECT * FROM " + testTable1.getTableName();
        String tab2Select = "SELECT * FROM " + testTable2.getTableName();

        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(tab2Select, tab1Select);

        assertThat(insertStatements.get(0)).contains(testTable1.getTableName());
        assertThat(insertStatements.get(1)).contains(testTable2.getTableName());

    }

}
