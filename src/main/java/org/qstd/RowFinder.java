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
package org.qstd;

import org.qstd.dbtype.DatabaseType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

class RowFinder {

    private final DataSource dataSource;

    private final DatabaseType dbType;

    RowFinder(DataSource dataSource, DatabaseType dbType) {
        this.dataSource = dataSource;
        this.dbType = dbType;
    }

    DatasetRow findOneRowFrom(String tableName
                            , Collection<String> columnNamesToSearch
                            , DatasetRow rowToSearch) {

        SqlQuery missingColumnValuesQuery =
                SqlQuery.buildFromRow(columnNamesToSearch, rowToSearch, dbType);

        DatasetRow missingColumnValues = DatasetRow.ofTable(tableName);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement missingColumnStatement = PreparedStatementBuilder.buildFrom(missingColumnValuesQuery, connection)) {
            ResultSet queryResult = missingColumnStatement.executeQuery();

            queryResult.next(); // We keep only the first row found

            for (String missingColumnName : columnNamesToSearch) {
                Object columnValue = queryResult.getObject(missingColumnName);
                missingColumnValues.addColumnValue(missingColumnName, columnValue);
            }
        } catch (SQLException sqlException) {
            System.err.println("Unable to execute " + missingColumnValuesQuery);
            sqlException.printStackTrace();
        }
        return missingColumnValues;
    }

}
