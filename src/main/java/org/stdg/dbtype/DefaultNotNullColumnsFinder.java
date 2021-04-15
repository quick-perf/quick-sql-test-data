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

import org.stdg.NotNullColumnsFinder;
import org.stdg.PreparedStatementBuilder;
import org.stdg.SqlQuery;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class DefaultNotNullColumnsFinder implements NotNullColumnsFinder {

    private static final SqlQuery NOT_NULL_COLUMNS_QUERY = new SqlQuery(
            "select table_schema as table_schema,\n" +
                    "       table_name   as table_name,\n" +
                    "       column_name  as not_null_column\n" +
                    "from information_schema.columns\n" +
                    "where is_nullable = 'NO'\n" +
                    "  AND table_name=?");

    private final DataSource dataSource;

    DefaultNotNullColumnsFinder(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<String> findNotNullColumnsOf(String tableName) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement columnOrderStatement = PreparedStatementBuilder.buildFrom(NOT_NULL_COLUMNS_QUERY, connection)) {
            columnOrderStatement.setString(1, tableName);
            ResultSet queryResult = columnOrderStatement.executeQuery();
            return findNotNullColumnsFrom(queryResult);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return Collections.emptyList();
    }

    private List<String> findNotNullColumnsFrom(ResultSet resultSet) throws SQLException {
        List<String> notNullColumns = new ArrayList<>();
        while (resultSet.next()) {
            String columnName = resultSet.getString(3);
            notNullColumns.add(columnName);
        }
        return notNullColumns;
    }

}
