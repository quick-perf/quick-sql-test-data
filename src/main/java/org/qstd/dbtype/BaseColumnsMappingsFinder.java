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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

class BaseColumnsMappingsFinder implements ColumnsMappingsFinder {

    private final DataSource dataSource;

    private final SqlQuery columnsMappingQuery;

    public BaseColumnsMappingsFinder(DataSource dataSource, SqlQuery columnsMappingQuery) {
        this.dataSource = dataSource;
        this.columnsMappingQuery = columnsMappingQuery;
    }

    @Override
    public ColumnsMappingGroup findColumnsMappingsOf(String tableName) {

        Collection<ColumnsMapping> columnsMappings = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement referencedTablesStatement = PreparedStatementBuilder.buildFrom(columnsMappingQuery, connection)) {
            referencedTablesStatement.setString(1, tableName);

            ResultSet queryResult = referencedTablesStatement.executeQuery();

            while (queryResult.next()) {
                ColumnsMapping columnsMapping = buildColumnsMappingFrom(queryResult);
                columnsMappings.add(columnsMapping);
            }


        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }


        return new ColumnsMappingGroup(columnsMappings);

    }

    private ColumnsMapping buildColumnsMappingFrom(ResultSet queryResult) throws SQLException {
        String firstTableSchema = queryResult.getString(1);
        String firstTableName = queryResult.getString(2);
        String firstTableColumn = queryResult.getString(3);

        ColumnMappingPart columnMappingPart1
                = new ColumnMappingPart(firstTableSchema, firstTableName, firstTableColumn);

        String secondTableSchema = queryResult.getString(4);
        String secondTableName = queryResult.getString(5);
        String secondTableColumn = queryResult.getString(6);

        ColumnMappingPart columnMappingPart2
                = new ColumnMappingPart(secondTableSchema, secondTableName, secondTableColumn);

        return new ColumnsMapping(columnMappingPart1, columnMappingPart2);
    }

}
