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
package org.qstd.dbtype;

import org.qstd.ColumnOrdersFinder;
import org.qstd.SqlQuery;

import javax.sql.DataSource;
import java.util.List;

class DefaultColumnOrdersFinder implements ColumnOrdersFinder {

    private static final SqlQuery COLUMN_ORDER_QUERY = new SqlQuery(
            " select table_schema," +
                    "        table_name," +
                    "        column_name," +
                    "        ordinal_position as position" +
                    " from information_schema.columns" +
                    " where table_name=?" +
                    " order by position");

    private BaseColumnOrdersFinder delegate;

    DefaultColumnOrdersFinder(DataSource dataSource) {
        delegate = new BaseColumnOrdersFinder(dataSource, COLUMN_ORDER_QUERY);
    }

    @Override
    public List<String> findDatabaseColumnOrdersOf(String tableName) {
        return delegate.findDatabaseColumnOrdersOf(tableName);
    }

}
