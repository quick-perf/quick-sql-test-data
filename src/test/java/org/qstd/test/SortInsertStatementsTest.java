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

import org.junit.jupiter.api.RepeatedTest;
import org.qstd.QuickSqlTestData;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.qstd.test.TestTable.TestTableAssert.assertThat;
import static org.qstd.test.TestTable.buildUniqueTable;

public class SortInsertStatementsTest extends H2Config {

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

        String playerSelect = "SELECT * FROM " + playerTable.getTableName();
        String teamSelect = "SELECT * FROM " + teamTable.getTableName();

        // WHEN
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        String insertScript = quickSqlTestData.generateInsertScriptFor(playerSelect, teamSelect);

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

        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);
        List<String> insertStatements = quickSqlTestData.generateInsertListFor(tab2Select, tab1Select);

        assertThat(insertStatements.get(0)).contains(testTable1.getTableName());
        assertThat(insertStatements.get(1)).contains(testTable2.getTableName());

    }

}
