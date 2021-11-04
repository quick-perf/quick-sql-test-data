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

import org.qstd.ColumnOrdersFinder;
import org.qstd.PreparedStatementBuilder;
import org.qstd.SqlQuery;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class BaseColumnOrdersFinder implements ColumnOrdersFinder {

    private final DataSource dataSource;

    private final SqlQuery notNullColumnsQuery;

    BaseColumnOrdersFinder(DataSource dataSource, SqlQuery notNullColumnsQuery) {
        this.dataSource = dataSource;
        this.notNullColumnsQuery = notNullColumnsQuery;
    }

    @Override
    public List<String> findDatabaseColumnOrdersOf(String tableName) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement columnOrderStatement = PreparedStatementBuilder.buildFrom(notNullColumnsQuery, connection)) {
            columnOrderStatement.setString(1, tableName);
            ResultSet queryResult = columnOrderStatement.executeQuery();
            return findColumnOrderFrom(queryResult);
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return Collections.emptyList();
    }

    private List<String> findColumnOrderFrom(ResultSet queryResult) throws SQLException {
        List<String> columnOrder = new ArrayList<>();
        while (queryResult.next()) {
            String columnName = queryResult.getString(3);
            columnOrder.add(columnName);
        }
        return columnOrder;
    }

}
