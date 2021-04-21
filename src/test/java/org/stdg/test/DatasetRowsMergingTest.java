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

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class DatasetRowsMergingTest extends H2Configuration {

    @Test public void
    should_merge_dataset_rows_if_columns_in_common_have_same_values() {

        // GIVEN
        TestTable table =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Table"
                                          , "col1 varchar(255)"
                                          + ", col2 varchar(255)"
                                          + ", col3 varchar(255)"
                                          )
                        .create()
                        .insertValues("'val1', 'val2', 'val3'");

        // WHEN
        String playerTableName = table.getTableName();
        String select1 = "SELECT col1, col2 FROM " + playerTableName;
        String select2 = "SELECT col1, col3 FROM " + playerTableName;
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(select1, select2);

        // THEN
        table.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(table).withScript(insertScript).hasNumberOfRows(1);
        TestTable.TestTableAssert.assertThat(table).row(0).column(0).hasValues("val1");
        TestTable.TestTableAssert.assertThat(table).row(0).column(1).hasValues("val2");
        TestTable.TestTableAssert.assertThat(table).row(0).column(2).hasValues("val3");

    }

    @Test public void
    should_merge_dataset_rows_in_case_of_joined_rows() {

        // GIVEN
        TestTable sponsorTable =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "Sponsor"
                                          , "  id bigint" +
                                          ",  name varchar(255) not null" +
                                          ", country varchar(255)" +
                                          ", primary key (id)"
                                          )
                .create()
                .insertValues("1, 'Sponsor name', 'France'");

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
                                          + ", primary key (id)"
                                          )
                .create()
                .alter(playerTeamForeignKey)
                .insertValues("1, 'Paul', 'Pogba', 1");

        // WHEN
        String playerSelect = "SELECT * FROM " + playerTable.getTableName();
        String sponsorSelect = "SELECT * FROM " + sponsorTable.getTableName();
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(playerSelect, sponsorSelect);

        // THEN
        playerTable.drop();
        teamTable.drop();
        sponsorTable.drop().create();
        teamTable.create().alter(teamSponsorForeignKey);
        playerTable.create().alter(playerTeamForeignKey);
        SQL_EXECUTOR.execute(insertScript);

        TestTable.TestTableAssert.assertThat(sponsorTable).withScript(insertScript).hasNumberOfRows(1);
        TestTable.TestTableAssert.assertThat(sponsorTable).row(0).column(0).hasValues(1);
        TestTable.TestTableAssert.assertThat(sponsorTable).row(0).column(1).hasValues("Sponsor name");
        TestTable.TestTableAssert.assertThat(sponsorTable).row(0).column(2).hasValues("France");

    }

    private int generateRandomPositiveInt() {
        Random random = new Random();
        return Math.abs(random.nextInt());
    }

}
