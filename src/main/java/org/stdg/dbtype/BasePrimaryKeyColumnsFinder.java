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

import org.stdg.PreparedStatementBuilder;
import org.stdg.PrimaryKeyColumnsFinder;
import org.stdg.SqlQuery;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BasePrimaryKeyColumnsFinder implements PrimaryKeyColumnsFinder {

    private final DataSource dataSource;

    private final SqlQuery primaryKeyColumnsQuery;

    public BasePrimaryKeyColumnsFinder(DataSource dataSource, SqlQuery primaryKeyColumnsQuery) {
        this.dataSource = dataSource;
        this.primaryKeyColumnsQuery = primaryKeyColumnsQuery;
    }

    @Override
    public List<String> findPrimaryColumnsOf(String tableName) {

        List<String> primaryKeyColumns = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement primaryKeyColumnsStatement = PreparedStatementBuilder.buildFrom(primaryKeyColumnsQuery, connection)) {

            primaryKeyColumnsStatement.setString(1, tableName);
            ResultSet queryResult = primaryKeyColumnsStatement.executeQuery();

            while(queryResult.next()) {
                String column = queryResult.getString(4);
                primaryKeyColumns.add(column);
            }

        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }

        return primaryKeyColumns;

    }

}
