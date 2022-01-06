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

import org.qstd.ReferencedTableSet;
import org.qstd.ReferencedTablesFinder;
import org.qstd.SqlQuery;

import javax.sql.DataSource;

class PostgreSqlMariaDbReferencedTablesFinder implements ReferencedTablesFinder {

    private static final SqlQuery REFERENCED_TABLES_QUERY = new SqlQuery("with \n" +
            "    recursive parent_child_tree as\n" +
            "    (\n" +
            "    with parent_child as\n" +
            "        (\n" +
            "        select distinct\n" +
            "            child.table_schema  as table_schema,\n" +
            "            child.table_name    as table_name,\n" +
            "            parent.table_schema as ref_table_schema,\n" +
            "            parent.table_name   as ref_table_name\n" +
            "        from information_schema.referential_constraints rco\n" +
            "        join information_schema.table_constraints child\n" +
            "             on rco.constraint_name = child.constraint_name\n" +
            "             and rco.constraint_schema = child.table_schema\n" +
            "        join information_schema.table_constraints parent\n" +
            "             on rco.unique_constraint_name = parent.constraint_name\n" +
            "             and rco.unique_constraint_schema = parent.table_schema\n" +
            "        where child.table_name != parent.table_name\n" +
            "        )\n" +
            "    select table_name, ref_table_name, 1 as level\n" +
            "      from parent_child\n" +
            "     where table_name=?\n" +
            "    UNION\n" +
            "    select pc.table_name, pc.ref_table_name, pct.level + 1 as level\n" +
            "      from parent_child_tree pct\n" +
            "      join parent_child pc on (pc.table_name = pct.ref_table_name)\n" +
            "    )\n" +
            "select *\n" +
            "from parent_child_tree\n" +
            "order by level desc");

    private final BaseReferencedTablesFinder referencedTablesFinder;

    PostgreSqlMariaDbReferencedTablesFinder(DataSource dataSource) {
        this.referencedTablesFinder = new BaseReferencedTablesFinder(dataSource, REFERENCED_TABLES_QUERY);
    }

    @Override
    public ReferencedTableSet findReferencedTablesOf(String tableName) {
        return referencedTablesFinder.findReferencedTablesOf(tableName);
    }

}
