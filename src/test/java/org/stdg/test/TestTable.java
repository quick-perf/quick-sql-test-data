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

import org.assertj.core.api.AbstractAssert;
import org.assertj.db.api.Assertions;
import org.assertj.db.api.TableAssert;
import org.assertj.db.api.TableRowAssert;
import org.assertj.db.type.Table;

import javax.sql.DataSource;
import java.util.List;
import java.util.Random;

class TestTable {

    private final String tableName;

    private final String creationScript;

    private final DataSource dataSource;

    private final SqlExecutor sqlExecutor;

    TestTable(DataSource dataSource, String tableName, String creationScript) {
        this.tableName = tableName;
        this.creationScript = creationScript;
        this.dataSource = dataSource;
        this.sqlExecutor = new SqlExecutor(dataSource);
    }

    static TestTable buildUniqueTable(DataSource dataSource
                                    , String baseTableName
                                    , String colDescsAndConstraints) {
        String tableName = buildUniqueTableName(baseTableName);
        String creationScript = "create table " + tableName
                             + "(" + colDescsAndConstraints + ")";
        return new TestTable(dataSource, tableName, creationScript);
    }

    private static String buildUniqueTableName(String baseTableName) {
        return baseTableName + "_" + generateRandomPositiveInt();
    }

    private static int generateRandomPositiveInt() {
        Random random = new Random();
        return Math.abs(random.nextInt());
    }

    TestTable recreate() {
        drop();
        create();
        return this;
    }

    TestTable drop() {
        sqlExecutor.execute("drop table " + tableName);
        return this;
    }

    TestTable create() {
        sqlExecutor.execute(creationScript);
        return this;
    }

    TestTable insertValues(String valuesSeparatedWithCommas) {
        String insert = "INSERT INTO " + tableName + " VALUES (" +valuesSeparatedWithCommas + ")";
        sqlExecutor.execute(insert);
        return this;
    }

    String getTableName() {
        return tableName;
    }

    TestTable alter(String alterCode) {
        sqlExecutor.execute("alter table " + tableName + " " + alterCode);
        return this;
    }

    static class TestTableAssert extends AbstractAssert<TestTableAssert, TestTable> {

        private static final String LINE_SEPARATOR = System.lineSeparator();
        private Table assertJDbTable;

        TestTableAssert(TestTable testTable, Class<?> selfType) {
            super(testTable, TestTableAssert.class);
        }

        static TestTableAssert assertThat(TestTable testTable) {
            TestTableAssert testTableAssert = new TestTableAssert(testTable, TestTableAssert.class);
            testTableAssert.assertJDbTable = new Table(testTable.dataSource, testTable.tableName);
            return testTableAssert;
        }

        TableAssert hasNumberOfRows(int expected) {
            return Assertions.assertThat(assertJDbTable).hasNumberOfRows(expected);
        }

        TableAssert withScript(String sqlScript) {
            String description =  LINE_SEPARATOR
                                + "SQL script: "
                                + LINE_SEPARATOR
                                + sqlScript
                                + LINE_SEPARATOR;
            return Assertions.assertThat(assertJDbTable).as(description);
        }

        TableRowAssert row(int index) {
            return Assertions.assertThat(assertJDbTable).row(index);
        }

        TableAssert withGeneratedInserts(List<String> generatedInsertStatements) {
            String description = LINE_SEPARATOR
                              + "Queries: "
                              + LINE_SEPARATOR
                              + String.join(LINE_SEPARATOR, generatedInsertStatements);
            return Assertions.assertThat(assertJDbTable).as(description);
        }
    }

}
