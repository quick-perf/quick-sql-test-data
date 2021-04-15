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

import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.junit.jupiter.api.*;
import org.quickperf.junit5.QuickPerfTest;
import org.quickperf.sql.annotation.ExpectJdbcQueryExecution;
import org.quickperf.sql.config.QuickPerfSqlDataSourceBuilder;
import org.stdg.SqlTestDataGenerator;

import javax.sql.DataSource;
import java.util.Random;

@QuickPerfTest
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class JdbcRoundtripTest {

    private static DataSource DATA_SOURCE;

    private static SqlExecutor SQL_EXECUTOR;
    private TestTable t1Table;
    private TestTable t2Table;
    private TestTable t3Table;
    private String t2TableConstraint;
    private String insertScript;

    @BeforeAll
    public static void beforeAll() {
        DataSource h2Datasource = DataSourceBuilder.build("jdbc:h2:mem:test", "user", "pwd");
        ProxyDataSource quickPerfProxyDataSource = QuickPerfSqlDataSourceBuilder.aDataSourceBuilder()
                .buildProxy(h2Datasource);
        DATA_SOURCE = quickPerfProxyDataSource;
        SQL_EXECUTOR = new SqlExecutor(DATA_SOURCE);
    }

    @BeforeEach
    public void prepare_test_data() {

        t1Table =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "t1"
                                          , "id_t1 bigint not null" +
                                          ",  c2_t1 varchar(255) not null" +
                                          ",  c3_t1 varchar(255) not null" +
                                          ",  c4_t1 varchar(255) not null" +
                                          ",  primary key (id_t1)"
                                          )
                        .create()
                        .insertValues("1, 't1_r1_c1_val', 't1_r1_c2_val', 't1_r1_c3_val'")
                        .insertValues("2, 't1_r2_c1_val', 't1_r2_c2_val', 't1_r2_c3_val'");

        t2TableConstraint = "add constraint t1_t2_fk" + generateRandomPositiveInt()
                          + " foreign key (t1_id)"
                          + " references " + t1Table.getTableName();

        t2Table =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "t2"
                                          , "id_t2 bigint not null" +
                                          ",  t1_id bigint not null" +
                                          ",  c1_t2 varchar(255) not null" +
                                          ",  c2_t2 varchar(255) not null" +
                                          ",  c3_t2 varchar(255) not null" +
                                          ", primary key (id_t2)")
                        .create()
                        .alter(t2TableConstraint)
                        .insertValues("1, 1, 't2_r1_c1_val', 't2_r1_c2_val', 't2_r1_c3_val'")
                        .insertValues("2, 2, 't2_r2_c1_val', 't2_r2_c2_val', 't2_r2_c3_val'");

        t3Table =
                TestTable.buildUniqueTable(DATA_SOURCE
                                          , "t3"
                                          , "id_t3 bigint not null" +
                                          ",  c2_t3 varchar(255) not null" +
                                          ",  c3_t3 varchar(255) not null" +
                                          ",  c4_t3 varchar(255) not null" +
                                          ", primary key (id_t3)")
                        .create()
                        .insertValues("1, 't3_r1_c1_val', 't3_r1_c2_val', 't3_r1_c3_val'")
                        .insertValues("2, 't3_r2_c1_val', 't3_r2_c2_val', 't3_r2_c3_val'");

    }

    private int generateRandomPositiveInt() {
        Random random = new Random();
        return Math.abs(random.nextInt());
    }

    @ExpectJdbcQueryExecution(23)
    @Test public void
    should_limit_jdbc_roundtrips() {
        String t2Select = "SELECT c1_t2 FROM " + t2Table.getTableName();
        String t3Select = "SELECT c2_t3 FROM " + t3Table.getTableName();
        SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(DATA_SOURCE);
        insertScript = sqlTestDataGenerator.generateInsertScriptFor(t2Select, t3Select);
    }

    @AfterEach
    public void check_inserted_data() {
        t3Table.recreate();
        t2Table.drop();
        t1Table.drop().create();
        t2Table.create().alter(t2TableConstraint);
        SQL_EXECUTOR.execute(insertScript);
        TestTable.TestTableAssert.assertThat(t1Table).withScript(insertScript).hasNumberOfRows(2);
        TestTable.TestTableAssert.assertThat(t2Table).withScript(insertScript).hasNumberOfRows(2);
        TestTable.TestTableAssert.assertThat(t3Table).withScript(insertScript).hasNumberOfRows(2);
    }

}
