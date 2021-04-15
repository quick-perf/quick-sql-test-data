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

import org.stdg.*;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.List;

class PostgreSqlMetadataFinder implements DatabaseMetadataFinder {

    private static final SqlQuery POSTGRE_SQL_COLUMNS_MAPPINGS_QUERY = new SqlQuery("select\n" +
            "       tc.table_schema     as table_schema,\n" +
            "       tc.table_name       as table_name,\n" +
            "       kcu.column_name     as column_name,\n" +
            "       ccu.table_schema    as ref_table_schema,\n" +
            "       ccu.table_name      as ref_table_name,\n" +
            "       ccu.column_name     as ref_column_name\n" +
            "  from information_schema.table_constraints as tc\n" +
            "  join information_schema.key_column_usage as kcu\n" +
            "       using (constraint_schema, constraint_name, table_schema)\n" +
            "  join information_schema.constraint_column_usage as ccu\n" +
            "       using (constraint_schema, constraint_name, table_schema)\n" +
            "where tc.constraint_type = 'FOREIGN KEY' and tc.table_name=?");

    private final DefaultColumnOrdersFinder defaultColumnOrdersFinder;

    private final NotNullColumnsFinder defaultNotNullColumnsFinder;

    private final PostgreSqlMariaDbReferencedTablesFinder postgreSqlMariaDbReferencedTablesFinder;

    private final ColumnsMappingsFinder postgreSqlColumnsMappingsFinder;

    private final PrimaryKeyColumnsFinder primaryKeyColumnsFinder;

    PostgreSqlMetadataFinder(DataSource dataSource) {
        this.defaultColumnOrdersFinder = new DefaultColumnOrdersFinder(dataSource);
        this.defaultNotNullColumnsFinder = new DefaultNotNullColumnsFinder(dataSource);
        this.postgreSqlMariaDbReferencedTablesFinder = new PostgreSqlMariaDbReferencedTablesFinder(dataSource);
        this.postgreSqlColumnsMappingsFinder = new BaseColumnsMappingsFinder(dataSource, POSTGRE_SQL_COLUMNS_MAPPINGS_QUERY);
        this.primaryKeyColumnsFinder = new DefaultPrimaryKeyColumnsFinder(dataSource);
    }

    @Override
    public List<String> findDatabaseColumnOrdersOf(String tableName) {
        return defaultColumnOrdersFinder.findDatabaseColumnOrdersOf(tableName);
    }

    @Override
    public Collection<String> findNotNullColumnsOf(String tableName) {
        return defaultNotNullColumnsFinder.findNotNullColumnsOf(tableName);
    }

    @Override
    public ReferencedTableSet findReferencedTablesOf(String tableName) {
        return postgreSqlMariaDbReferencedTablesFinder.findReferencedTablesOf(tableName);
    }

    @Override
    public ColumnsMappingGroup findColumnsMappingsOf(String tableName) {
        return postgreSqlColumnsMappingsFinder.findColumnsMappingsOf(tableName);
    }

    @Override
    public List<String> findPrimaryColumnsOf(String tableName) {
        return primaryKeyColumnsFinder.findPrimaryColumnsOf(tableName);
    }

}
