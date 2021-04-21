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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.stdg.SqlTestDataGenerator;
import org.testcontainers.containers.MariaDBContainer;

import javax.sql.DataSource;
import java.util.List;
import java.util.Random;

import static org.stdg.test.TestTable.TestTableAssert.assertThat;
import static org.stdg.test.TestTable.buildUniqueTable;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class MariaDBSlowTest {

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

    @Test public void
    should_sort_insert_statements_following_table_dependencies() {

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
                                     + " references " + teamTable.getTableName() + " (id)";
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

        // WHEN
        String playerSelect = "SELECT * FROM " + playerTable.getTableName();
        String teamSelect = "SELECT * FROM " + teamTable.getTableName();
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(playerSelect, teamSelect);

        // THEN
        playerTable.drop();
        teamTable.drop().create();
        playerTable.create().alter(playerTableConstraint);
        SQL_EXECUTOR.execute(insertStatements);
        assertThat(playerTable).withGeneratedInserts(insertStatements).hasNumberOfRows(1);
        assertThat(teamTable).hasNumberOfRows(1);

    }

    private int generateRandomPositiveInt() {
        Random random = new Random();
        return Math.abs(random.nextInt());
    }

}
