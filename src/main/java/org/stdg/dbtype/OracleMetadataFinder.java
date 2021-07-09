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
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class OracleMetadataFinder implements DatabaseMetadataFinder {

    private static final SqlQuery COLUMNS_ORDER_QUERY = new SqlQuery(
            "select owner        as table_schema," +
                    "       table_name   as table_name," +
                    "       column_name  as column_name," +
                    "       column_id    as position" +
                    " from all_tab_columns\n" +
                    " where table_name = ?" +
                    " order by position"
    );

    private static final SqlQuery NOT_NULL_COLUMNS_QUERY = new SqlQuery(
            "select owner as table_schema," +
                    " table_name  as table_name," +
                    " column_name as mandatory_column" +
                    " from all_tab_columns" +
                    " where table_name = ?" +
                    " and nullable = 'N'"
    );

    private static final SqlQuery REFERENCED_TABLES_QUERY = new SqlQuery(
            "select\n" +
                    "       table_name,\n" +
                    "       ref_table_name,\n" +
                    "       level\n" +
                    "  from\n" +
                    "       (\n" +
                    "        select\n" +
                    "              c.owner           as table_schema,\n" +
                    "              c.table_name,\n" +
                    "              c.r_owner         as ref_table_schema,\n" +
                    "              ref_c.table_name  as ref_table_name\n" +
                    "          from\n" +
                    "              all_constraints c\n" +
                    "              inner join all_constraints ref_c on ref_c.constraint_name = c.r_constraint_name\n" +
                    "        where\n" +
                    "              c.constraint_type = 'R'\n" +
                    "          and c.table_name != ref_c.table_name\n" +
                    "       )\n" +
                    "  start with table_name = ?\n" +
                    "  connect by table_name = prior ref_table_name\n" +
                    "order by level desc"
    );

    private static final SqlQuery COLUMNS_MAPPING_QUERY = new SqlQuery(
            "select\n" +
                    "       c.owner              as table_schema,\n" +
                    "       c.table_name,\n" +
                    "       col.column_name,\n" +
                    "       c.r_owner            as ref_table_schema,\n" +
                    "       ref_col.table_name   as ref_table_name,\n" +
                    "       ref_col.column_name  as ref_column_name\n" +
                    "  from\n" +
                    "       all_constraints c\n" +
                    "       inner join all_cons_columns col on col.owner = c.owner\n" +
                    "                                       and col.constraint_name = c.constraint_name\n" +
                    "       inner join all_cons_columns ref_col on ref_col.owner = c.r_owner\n" +
                    "                                           and ref_col.constraint_name = c.r_constraint_name\n" +
                    "                                           and ref_col.position = col.position\n" +
                    " where \n" +
                    "       c.table_name = ?\n" +
                    "   and c.constraint_type = 'R'");

    private static final SqlQuery PRIMARY_KEY_QUERY = new SqlQuery(
            "select\n" +
                    "       c.owner as table_schema,\n" +
                    "       c.table_name,\n" +
                    "       c.constraint_name,\n" +
                    "       col.column_name,\n" +
                    "       col.position\n" +
                    "  from\n" +
                    "       all_constraints c\n" +
                    "       inner join all_cons_columns col on col.owner = c.owner\n" +
                    "                                      and col.constraint_name = c.constraint_name\n" +
                    " where c.table_name = ?\n" +
                    "   and c.constraint_type = 'P'\n" +
                    "   order by position");

    private final BaseColumnOrdersFinder columnOrdersFinder;

    private final NotNullColumnsFinder notNullColumnsFinder;

    private final ReferencedTablesFinder referencedTablesFinder;

    private final BaseColumnsMappingsFinder columnsMappingsFinder;

    private final PrimaryKeyColumnsFinder primaryKeyColumnsFinder;

    @Override
    public Function<String, String> getFunctionToHaveMetadataTableName() {
        return tableName -> tableName.toUpperCase();
    }

    OracleMetadataFinder(DataSource dataSource) {
        columnOrdersFinder = new BaseColumnOrdersFinder(dataSource, COLUMNS_ORDER_QUERY);
        notNullColumnsFinder = new BaseNotNullColumnsFinder(dataSource, NOT_NULL_COLUMNS_QUERY);
        referencedTablesFinder = new BaseReferencedTablesFinder(dataSource, REFERENCED_TABLES_QUERY);
        columnsMappingsFinder = new BaseColumnsMappingsFinder(dataSource, COLUMNS_MAPPING_QUERY);
        primaryKeyColumnsFinder = new BasePrimaryKeyColumnsFinder(dataSource, PRIMARY_KEY_QUERY);
    }

    @Override
    public List<String> findDatabaseColumnOrdersOf(String tableName) {
        return columnOrdersFinder.findDatabaseColumnOrdersOf(tableName);
    }

    @Override
    public ColumnsMappingGroup findColumnsMappingsOf(String tableName) {
        return columnsMappingsFinder.findColumnsMappingsOf(tableName);
    }

    @Override
    public Collection<String> findNotNullColumnsOf(String tableName) {
        return notNullColumnsFinder.findNotNullColumnsOf(tableName);
    }

    @Override
    public List<String> findPrimaryColumnsOf(String tableName) {
        return primaryKeyColumnsFinder.findPrimaryColumnsOf(tableName);
    }

    @Override
    public ReferencedTableSet findReferencedTablesOf(String tableName) {
        return referencedTablesFinder.findReferencedTablesOf(tableName);
    }

}
