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

import javax.sql.DataSource;

class H2Configuration {

   static DataSource DATA_SOURCE;

   static SqlExecutor SQL_EXECUTOR;

    @BeforeAll
    public static void beforeAll() {
        DATA_SOURCE = DataSourceBuilder.build("jdbc:h2:mem:test", "user", "pwd");
        SQL_EXECUTOR = new SqlExecutor(DATA_SOURCE);
    }

}
