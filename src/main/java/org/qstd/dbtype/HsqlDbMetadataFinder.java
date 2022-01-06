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

import org.qstd.*;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.List;

class HsqlDbMetadataFinder implements DatabaseMetadataFinder {

    private static final SqlQuery HSQL_DB_REFERENCED_TABLES_QUERY = new SqlQuery(
            "with\n" +
                    "    recursive parent_child_tree (table_name, ref_table_name, level) as\n" +
                    "    (\n" +
                    "    select distinct\n" +
                    "        child.table_name     as table_name,\n" +
                    "        parent.table_name    as ref_table_name,\n" +
                    "        1                    as level\n" +
                    "    from information_schema.table_constraints child\n" +
                    "    join information_schema.referential_constraints rco\n" +
                    "          on rco.constraint_name = child.constraint_name\n" +
                    "    join information_schema.table_constraints parent\n" +
                    "          on parent.constraint_name = rco.unique_constraint_name\n" +
                    "    where\n" +
                    "        child.table_name != parent.table_name and\n" +
                    "        child.table_name=?\n" +
                    "    UNION\n" +
                    "    select pc.table_name, pc.ref_table_name, pct.level + 1 as level\n" +
                    "    from\n" +
                    "        (\n" +
                    "        select distinct\n" +
                    "            child.table_name     as table_name,\n" +
                    "            parent.table_name    as ref_table_name,\n" +
                    "            1                    as level\n" +
                    "        from information_schema.table_constraints child\n" +
                    "        join information_schema.referential_constraints rco\n" +
                    "            on rco.constraint_name = child.constraint_name\n" +
                    "        join information_schema.table_constraints parent\n" +
                    "            on parent.constraint_name = rco.unique_constraint_name\n" +
                    "        where\n" +
                    "            child.table_name != parent.table_name\n" +
                    "        ) pc\n" +
                    "    join parent_child_tree pct on (pc.table_name = pct.ref_table_name)\n" +
                    "    )\n" +
                    "select distinct *\n" +
                    "from parent_child_tree\n" +
                    "order by level desc");

    private static final SqlQuery HSQL_DB_COLUMNS_MAPPINGS_QUERY = new SqlQuery(
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
                    "where child_constraint.constraint_type = 'FOREIGN KEY' and child_constraint.table_name=?"
    );

    private final DefaultColumnOrdersFinder defaultColumnOrdersFinder;

    private final NotNullColumnsFinder defaultNotNullColumnsFinder;

    private final ReferencedTablesFinder hsqlDbReferencedTablesFinder;

    private final ColumnsMappingsFinder hsqlDbColumnsMappingsFinder;

    private final DefaultPrimaryKeyColumnsFinder primaryKeyColumnsFinder;

    HsqlDbMetadataFinder(DataSource dataSource) {
        this.defaultColumnOrdersFinder = new DefaultColumnOrdersFinder(dataSource);
        this.defaultNotNullColumnsFinder = new DefaultNotNullColumnsFinder(dataSource);
        this.hsqlDbReferencedTablesFinder = new BaseReferencedTablesFinder(dataSource, HSQL_DB_REFERENCED_TABLES_QUERY);
        this.hsqlDbColumnsMappingsFinder = new BaseColumnsMappingsFinder(dataSource, HSQL_DB_COLUMNS_MAPPINGS_QUERY);
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
        return hsqlDbReferencedTablesFinder.findReferencedTablesOf(tableName);
    }

    @Override
    public ColumnsMappingGroup findColumnsMappingsOf(String tableName) {
        return hsqlDbColumnsMappingsFinder.findColumnsMappingsOf(tableName);
    }

    @Override
    public List<String> findPrimaryColumnsOf(String tableName) {
        return primaryKeyColumnsFinder.findPrimaryColumnsOf(tableName);
    }

}
