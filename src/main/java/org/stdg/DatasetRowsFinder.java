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

package org.stdg;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

class DatasetRowsFinder {

    private final DataSource dataSource;

    private final DatabaseMetadataFinder databaseMetadataFinder;

    DatasetRowsFinder(DataSource dataSource
                    , DatabaseMetadataFinder databaseMetadataFinder) {
        this.dataSource = dataSource;
        this.databaseMetadataFinder = databaseMetadataFinder;
    }

    List<DatasetRow> findDatasetRowsFrom(List<SqlQuery> sqlQueries) {

        DatasetRowSet datasetRowSet = new DatasetRowSet(dataSource, databaseMetadataFinder);

        for (SqlQuery sqlQuery : sqlQueries) {
            List<DatasetRow> datasetRowsForQuery = execute(sqlQuery);
            for (DatasetRow datasetRow : datasetRowsForQuery) {
                datasetRowSet.add(datasetRow);
            }
        }

        return datasetRowSet.sort();

    }

    private List<DatasetRow> execute(SqlQuery sqlQuery) {

        List<DatasetRow> datasetRowsToReturn = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement selectStatement = PreparedStatementBuilder.buildFrom(sqlQuery, connection)) {

            ResultSet resultSet = selectStatement.executeQuery();

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            int columnCount = resultSetMetaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, DatasetRow> rowByTableName = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {

                    String tableName = findTableName(sqlQuery, resultSetMetaData, i);

                    String column = resultSetMetaData.getColumnName(i);
                    Object value = resultSet.getObject(i);

                    DatasetRow datasetRow = rowByTableName.computeIfAbsent(tableName
                                                                         , t -> DatasetRow.ofTable(tableName));
                    datasetRow.addColumnValue(column, value);

                }

                Set<Map.Entry<String, DatasetRow>> tableNameEntries = rowByTableName.entrySet();

                for (Map.Entry<String, DatasetRow> tableNameEntry : tableNameEntries) {
                    DatasetRow datasetRow = tableNameEntry.getValue();
                    datasetRowsToReturn.add(datasetRow);
                }
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }

        return datasetRowsToReturn;

    }

    private String findTableName(SqlQuery sqlQuery, ResultSetMetaData resultSetMetaData, int i) throws SQLException {
        String tableName = resultSetMetaData.getTableName(i);

        if(tableName.isEmpty()) {
            String queryAsString = sqlQuery.getQueryAsString();
            tableName = extractTableNameFrom(queryAsString);
        }
        return tableName;
    }

    private String extractTableNameFrom(String sqlQueryAsString) {
        try {
            Statement statement = CCJSqlParserUtil.parse(sqlQueryAsString);
            Select select = (Select) statement;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List<String> tableList = tablesNamesFinder.getTableList(select);
            if(tableList.size() == 1) {
                return tableList.get(0);
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return "";
    }

}
