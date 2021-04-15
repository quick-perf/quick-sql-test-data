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

package org.stdg.dbtype;

import org.stdg.PrimaryKeyColumnsFinder;
import org.stdg.SqlQuery;

import javax.sql.DataSource;
import java.util.List;

class DefaultPrimaryKeyColumnsFinder implements PrimaryKeyColumnsFinder {

    private static final SqlQuery PRIMARY_COLUMNS_QUERY = new SqlQuery(
            "select \n" +
                    "     cons.table_schema,\n" +
                    "     cons.table_name,\n" +
                    "     cons.constraint_name,\n" +
                    "     cols.column_name,\n" +
                    "     cols.ordinal_position as position\n" +
                    " from information_schema.table_constraints cons\n" +
                    " join information_schema.key_column_usage cols on (cons.table_schema = cols.table_schema \n" +
                    " and cons.table_name = cols.table_name\n" +
                    " and cons.constraint_name = cols.constraint_name)\n" +
                    " where cons.table_name = ?" + "\n" +
                    " and cons.constraint_type='PRIMARY KEY'"
    );

    private final PrimaryKeyColumnsFinder delegate;

    DefaultPrimaryKeyColumnsFinder(DataSource dataSource) {
        delegate = new BasePrimaryKeyColumnsFinder(dataSource, PRIMARY_COLUMNS_QUERY);
    }

    @Override
    public List<String> findPrimaryColumnsOf(String tableName) {
        return delegate.findPrimaryColumnsOf(tableName);
    }

}
