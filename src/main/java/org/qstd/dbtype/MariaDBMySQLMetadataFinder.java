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

package org.qstd.dbtype;

import org.qstd.*;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

class MariaDBMySQLMetadataFinder implements DatabaseMetadataFinder {

    private static final SqlQuery MARIA_DB_MY_SQL_COLUMNS_MAPPINGS_QUERY
            = new SqlQuery("select\n" +
            "       child_constraint.table_schema            as table_schema,\n" +
            "       child_constraint.table_name              as table_name,\n" +
            "       child_cons_cols.column_name              as column_name,\n" +
            "       child_cons_cols.referenced_table_schema  as ref_table_schema,\n" +
            "       child_cons_cols.referenced_table_name    as ref_table_name,\n" +
            "       child_cons_cols.referenced_column_name   as ref_column_name\n" +
            "  from information_schema.table_constraints as child_constraint\n" +
            "  join information_schema.key_column_usage as child_cons_cols\n" +
            "       on (child_constraint.constraint_schema = child_cons_cols.constraint_schema\n" +
            "           and\n" +
            "           child_constraint.constraint_name = child_cons_cols.constraint_name\n" +
            "           and\n" +
            "           child_constraint.table_schema = child_cons_cols.table_schema\n" +
            "           and\n" +
            "           child_constraint.table_name = child_cons_cols.table_name)\n" +
            "where child_constraint.constraint_type = 'FOREIGN KEY' and child_constraint.table_name=?");

    private final DefaultColumnOrdersFinder defaultColumnOrdersFinder;

    private final NotNullColumnsFinder defaultNotNullColumnsFinder;

    private final PostgreSqlMariaDbReferencedTablesFinder postgreSqlMariaDbReferencedTablesFinder;

    private final BaseColumnsMappingsFinder mariaDbMySqlColumnsMappingsFinder;

    private final PrimaryKeyColumnsFinder primaryKeyColumnsFinder;

    MariaDBMySQLMetadataFinder(DataSource dataSource) {
        this.defaultColumnOrdersFinder = new DefaultColumnOrdersFinder(dataSource);
        this.defaultNotNullColumnsFinder = new DefaultNotNullColumnsFinder(dataSource);
        this.postgreSqlMariaDbReferencedTablesFinder = new PostgreSqlMariaDbReferencedTablesFinder(dataSource);
        this.mariaDbMySqlColumnsMappingsFinder = new BaseColumnsMappingsFinder(dataSource, MARIA_DB_MY_SQL_COLUMNS_MAPPINGS_QUERY);
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
        return mariaDbMySqlColumnsMappingsFinder.findColumnsMappingsOf(tableName);
    }

    @Override
    public List<String> findPrimaryColumnsOf(String tableName) {
        return Collections.emptyList();
    }

}
