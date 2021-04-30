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
import org.stdg.SqlTestDataGenerator;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static org.stdg.test.TestTable.TestTableAssert;
import static org.stdg.test.TestTable.buildUniqueTable;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class UpdateTest {

    private static DataSource DATA_SOURCE;

    private static SqlExecutor SQL_EXECUTOR;

    @BeforeAll
    public static void beforeAll() {
        DATA_SOURCE = DataSourceBuilder.build("jdbc:h2:mem:test", "user", "pwd");
        SQL_EXECUTOR = new SqlExecutor(DATA_SOURCE);
    }

    @Test public void
    should_generate_one_insert_if_all_rows_are_updated_and_no_mandatory_columns() {

        // GIVEN
        TestTable foodTable =
                buildUniqueTable(DATA_SOURCE
                        , "Food"
                        , "  id bigint"
                                + ", Dishname varchar(255)"
                                + ", Allergy varchar(255)"
                                + ", Price decimal")
                        .create()
                        .insertValues("1, 'Spaghetti Bolognese', 'cheese', 6.80")
                        .insertValues("2, 'Pizza Margherita', 'pasta', 7.99");

        // WHEN
        String foodTableName = foodTable.getTableName();
        String updateQuery = "UPDATE " + foodTableName
                + " SET Price = 7.00, Allergy = 'none'";

        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(updateQuery);

        // THEN
        foodTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTableAssert.assertThat(foodTable).withScript(insertScript).hasNumberOfRows(2);
        TestTableAssert.assertThat(foodTable).withScript(insertScript).row(0).hasValues(null, null, "cheese", 6.80);

    }

    @Test public void
    should_generate_insert_statements_from_update_containing_where_or_like() {

        // GIVEN
        TestTable foodTable =
                buildUniqueTable(DATA_SOURCE
                        , "Food"
                        , "  id bigint"
                                + ", Dishname varchar(255)"
                                + ", Allergy varchar(255)"
                                + ", Price decimal")
                        .create()
                        .insertValues("1, 'Spaghetti Bolognese', 'cheese', 6.80")
                        .insertValues("2, 'Pizza', 'pasta', 10.99");

        // WHEN
        String foodTableName = foodTable.getTableName();
        String updateQuery = "UPDATE " + foodTableName + " SET Price = 7.00"
                + " WHERE Allergy LIKE 'past%' OR Allergy = 'cheese'"
                + " OR Dishname = 'Pizza'";
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(updateQuery);

        // THEN
        foodTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTableAssert.assertThat(foodTable).withScript(insertScript).hasNumberOfRows(2);
        TestTableAssert.assertThat(foodTable).withScript(insertScript).column(0).hasOnlyNullValues();
        TestTableAssert.assertThat(foodTable).withScript(insertScript).column(1).containsValues("Spaghetti Bolognese", "Pizza");
        TestTableAssert.assertThat(foodTable).withScript(insertScript).column(2).containsValues("cheese", "pasta");
        TestTableAssert.assertThat(foodTable).withScript(insertScript).column(3).containsValues(6.80, 10.99);

    }


    @Test public void
    should_generate_insert_statements_from_update_containing_where_columnnames_values_positions_are_swapped() {

        // GIVEN
        TestTable foodTable =
                buildUniqueTable(DATA_SOURCE
                        , "Food"
                        , "  id bigint"
                                + ", Dishname varchar(255)"
                                + ", Allergy varchar(255)"
                                + ", Price decimal")
                        .create()
                        .insertValues("1, 'Spaghetti Bolognese', 'cheese', 6.80")
                        .insertValues("2, 'Pizza', 'pasta', 10.99");

        // WHEN
        String foodTableName = foodTable.getTableName();
        String updateQuery = "UPDATE " + foodTableName + " SET Price = 7.00"
                + " WHERE Allergy LIKE 'past%' OR 'cheese' = Allergy"
                + " OR 'Pizza' = Dishname";
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(updateQuery);

        // THEN
        foodTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTableAssert.assertThat(foodTable).withScript(insertScript).hasNumberOfRows(2);
        TestTableAssert.assertThat(foodTable).withScript(insertScript).column(0).hasOnlyNullValues();
        TestTableAssert.assertThat(foodTable).withScript(insertScript).column(1).containsValues("Spaghetti Bolognese", "Pizza");
        TestTableAssert.assertThat(foodTable).withScript(insertScript).column(2).containsValues("cheese", "pasta");
        TestTableAssert.assertThat(foodTable).withScript(insertScript).column(3).containsValues(6.80, 10.99);

    }

    @Test public void
    should_generate_insert_statement_from_update_containing_where_like_and() {

        // GIVEN
        TestTable foodTable =
                buildUniqueTable(DATA_SOURCE
                        , "Food"
                        , "  id bigint"
                                + ", Dishname varchar(255)"
                                + ", Allergy varchar(255)"
                                + ", Price decimal")
                        .create()
                        .insertValues("1, 'Spaghetti Bolognese', 'cheese', 6.80")
                        .insertValues("2, 'Pizza', 'pasta', 10.99");

        // WHEN
        String foodTableName = foodTable.getTableName();
        String updateQuery = "UPDATE " + foodTableName + " SET Price = 7.00"
                + " WHERE Allergy LIKE 'past%'"
                + " AND Dishname = 'Pizza'";
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        String insertScript = sqlTestDataGenerator.generateInsertScriptFor(updateQuery);

        // THEN
        foodTable.recreate();
        SQL_EXECUTOR.execute(insertScript);
        TestTableAssert.assertThat(foodTable).withScript(insertScript).hasNumberOfRows(1);
        TestTableAssert.assertThat(foodTable).withScript(insertScript).row(0).hasValues(null, "Pizza", "pasta", 10.99);

    }

}
