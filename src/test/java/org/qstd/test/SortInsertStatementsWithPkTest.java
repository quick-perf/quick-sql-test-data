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
 * Copyright 2021-2022 the original author or authors.
 */
package org.qstd.test;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.qstd.QuickSqlTestData;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.qstd.test.TestTable.buildUniqueTable;

public class SortInsertStatementsWithPkTest extends H2Config {

    @RepeatedTest(9) public void
    should_sort_insert_statements_following_a_primary_key() {

        TestTable table =
                buildUniqueTable(DATA_SOURCE
                                , "table_with_int_pk"
                                , "col_id integer," +
                                "colA  varchar(20), " +
                                "colB  varchar(20), " +
                                "constraint int_pk" + generateRandomPositiveInt() + " primary key (col_id)"
                                )
                .create()
                .insertValues("2, 'A', 'B'")
                .insertValues("10, 'C', 'D'")
                .insertValues("1, 'E', 'F'");

        String selectAll = "SELECT * FROM " + table.getTableName();
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);

        // WHEN
        List<String> insertStatements = quickSqlTestData.generateInsertListFor(selectAll);

        // THEN
        String insertStatementsAsString = insertStatements.toString();

        String firstQuery = insertStatements.get(0);
        assertThat(firstQuery).as(insertStatementsAsString).contains("VALUES(1");

        String secondQuery = insertStatements.get(1);
        assertThat(secondQuery).as(insertStatementsAsString).contains("VALUES(2");

        String thirdQuery = insertStatements.get(2);
        assertThat(thirdQuery).as(insertStatementsAsString).contains("VALUES(10");

    }

    // Not possible to both repeat and parameterize a JUnit 5 test
    @ParameterizedTest
    @ValueSource(strings = {"INT", "TINYINT", "SMALLINT", "BIGINT"}) // http://www.h2database.com/html/datatypes.html
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
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);

        // WHEN
        List<String> insertStatements = quickSqlTestData.generateInsertListFor(selectAll);

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
    should_sort_insert_statements_following_a_composite_primary_key() {

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
                .insertValues("1, 2, 'A', 'B'")
                .insertValues("1, 1, 'C', 'D'")
                .insertValues("2, 2, 'E', 'F'")
                .insertValues("2, 1, 'G', 'H'");

        String selectAll = "SELECT * FROM " + table.getTableName();
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);

        // WHEN
        List<String> insertStatements = quickSqlTestData.generateInsertListFor(selectAll);

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
    should_not_sort_insert_statements_if_the_primary_is_not_of_int_type() {

        // GIVEN
        TestTable table =
                buildUniqueTable(DATA_SOURCE
                                , "table_with_boolean_pk"
                                , "col_id " + "BOOLEAN" + "," +
                                "colA  varchar(20), " +
                                "colB  varchar(20), " +
                                "constraint int_pk" + generateRandomPositiveInt() + " primary key (col_id)"
                                )
                .create()
                .insertValues("FALSE, 'A', 'B'")
                .insertValues("TRUE, 'C', 'D'");

        String selectAll = "SELECT * FROM " + table.getTableName();
        QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(DATA_SOURCE);

        // WHEN
        List<String> insertStatements = quickSqlTestData.generateInsertListFor(selectAll);

        // THEN
        String insertStatementsAsString = insertStatements.toString();

        String firstQuery = insertStatements.get(0);
        assertThat(firstQuery).as(insertStatementsAsString).contains("VALUES(false");

        String secondQuery = insertStatements.get(1);
        assertThat(secondQuery).as(insertStatementsAsString).contains("VALUES(true");

    }

    private int generateRandomPositiveInt() {
        Random random = new Random();
        return Math.abs(random.nextInt());
    }

}
