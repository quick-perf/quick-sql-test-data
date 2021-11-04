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

class MSSQLServerMetadataFinder implements DatabaseMetadataFinder {

    private static final SqlQuery MS_SQL_SERVER_REFERENCED_TABLES_QUERY = new SqlQuery(
            "with\n" +
                    "    parent_child_tree as\n" +
                    "    (\n" +
                    "    select distinct\n" +
                    "           child.table_name    as table_name,\n" +
                    "           parent.table_name   as ref_table_name,\n" +
                    "           1 as level\n" +
                    "    from information_schema.referential_constraints rco\n" +
                    "    join information_schema.table_constraints child\n" +
                    "            on rco.constraint_name = child.constraint_name\n" +
                    "            and rco.constraint_schema = child.table_schema\n" +
                    "    join information_schema.table_constraints parent\n" +
                    "            on rco.unique_constraint_name = parent.constraint_name\n" +
                    "            and rco.unique_constraint_schema = parent.table_schema\n" +
                    "    where child.table_name != parent.table_name\n" +
                    "        and child.table_name=?\n" +
                    "    UNION ALL\n" +
                    "    select pc.table_name, pc.ref_table_name, pct.level + 1 as level\n" +
                    "    from\n" +
                    "        (\n" +
                    "        select \n" +
                    "               child.table_name    as table_name,\n" +
                    "               parent.table_name   as ref_table_name,\n" +
                    "               1 as level\n" +
                    "        from information_schema.referential_constraints rco\n" +
                    "        join information_schema.table_constraints child\n" +
                    "                on rco.constraint_name = child.constraint_name\n" +
                    "                and rco.constraint_schema = child.table_schema\n" +
                    "        join information_schema.table_constraints parent\n" +
                    "                on rco.unique_constraint_name = parent.constraint_name\n" +
                    "                and rco.unique_constraint_schema = parent.table_schema\n" +
                    "        where child.table_name != parent.table_name\n" +
                    "        ) pc\n" +
                    "    join parent_child_tree pct on (pc.table_name = pct.ref_table_name)\n" +
                    "    )\n" +
                    "select distinct *\n" +
                    "from parent_child_tree\n" +
                    "order by level desc");

    private static final SqlQuery MS_SQL_SERVER_COLUMNS_MAPPINGS_QUERY = new SqlQuery(
            "select\n" +
                    "       child_constraint.table_schema    as table_schema,\n" +
                    "       child_constraint.table_name      as table_name,\n" +
                    "       child_cons_cols.column_name      as column_name,\n" +
                    "       parent_cons_cols.table_schema    as ref_table_schema,\n" +
                    "       parent_cons_cols.table_name      as ref_table_name,\n" +
                    "       parent_cons_cols.column_name     as ref_column_name\n" +
                    "  from information_schema.table_constraints as child_constraint\n" +
                    "  join information_schema.key_column_usage as child_cons_cols\n" +
                    "       on (child_constraint.constraint_schema = child_cons_cols.constraint_schema\n" +
                    "           and\n" +
                    "           child_constraint.constraint_name = child_cons_cols.constraint_name\n" +
                    "           and\n" +
                    "           child_constraint.table_schema = child_cons_cols.table_schema)\n" +
                    "  join information_schema.referential_constraints as ref\n" +
                    "       on (child_constraint.constraint_schema = ref.constraint_schema\n" +
                    "           and\n" +
                    "           child_constraint.constraint_name = ref.constraint_name)\n" +
                    "  join information_schema.key_column_usage as parent_cons_cols\n" +
                    "       on (parent_cons_cols.constraint_schema = ref.unique_constraint_schema\n" +
                    "           and\n" +
                    "           parent_cons_cols.constraint_name = ref.unique_constraint_name)\n" +
                    "where child_constraint.constraint_type = 'FOREIGN KEY' and child_constraint.table_name=?");

    private final DefaultColumnOrdersFinder defaultColumnOrdersFinder;

    private final NotNullColumnsFinder defaultNotNullColumnsFinder;

    private final ReferencedTablesFinder mssqlServerReferencedTablesFinder;

    private final ColumnsMappingsFinder mssqlServerColumnsMappingsFinder;

    MSSQLServerMetadataFinder(DataSource dataSource) {
        this.defaultColumnOrdersFinder = new DefaultColumnOrdersFinder(dataSource);
        this.defaultNotNullColumnsFinder = new DefaultNotNullColumnsFinder(dataSource);
        this.mssqlServerReferencedTablesFinder = new BaseReferencedTablesFinder(dataSource, MS_SQL_SERVER_REFERENCED_TABLES_QUERY);
        this.mssqlServerColumnsMappingsFinder = new BaseColumnsMappingsFinder(dataSource, MS_SQL_SERVER_COLUMNS_MAPPINGS_QUERY);
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
        return mssqlServerReferencedTablesFinder.findReferencedTablesOf(tableName);
    }

    @Override
    public ColumnsMappingGroup findColumnsMappingsOf(String tableName) {
        return mssqlServerColumnsMappingsFinder.findColumnsMappingsOf(tableName);
    }

    @Override
    public List<String> findPrimaryColumnsOf(String tableName) {
        return Collections.emptyList();
    }

}
