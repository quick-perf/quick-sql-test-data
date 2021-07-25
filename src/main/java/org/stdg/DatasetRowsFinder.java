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

import static java.util.Collections.emptyList;
import static org.stdg.SelectTransformerFactory.createSelectTransformer;

class DatasetRowsFinder {

    private final DataSource dataSource;

    DatasetRowsFinder(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    Collection<DatasetRow> findDatasetRowsOf(SqlQuery sqlQuery) {

        SelectTransformer selectTransformer = createSelectTransformer(sqlQuery);
        Optional<SqlQuery> optionalSelectQuery = selectTransformer.toSelect(sqlQuery);

        if (optionalSelectQuery.isPresent()) {
            SqlQuery selectQuery = optionalSelectQuery.get();
            return execute(selectQuery);
        }

        return emptyList();
    }

    private Collection<DatasetRow> execute(SqlQuery sqlQuery) {

        List<DatasetRow> datasetRowsToReturn = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
             PreparedStatement selectStatement = PreparedStatementBuilder.buildFrom(sqlQuery, connection)) {

            ResultSet resultSet = selectStatement.executeQuery();

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();

            while (resultSet.next()) {
                Collection<DatasetRow> datasetRows =
                        buildDatasetRowsFrom(resultSet, resultSetMetaData
                                           , columnCount, sqlQuery);
                datasetRowsToReturn.addAll(datasetRows);
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }

        return datasetRowsToReturn;

    }

    private Collection<DatasetRow> buildDatasetRowsFrom(ResultSet resultSet, ResultSetMetaData resultSetMetaData
                                                      , int columnCount, SqlQuery sqlQuery) throws SQLException {
        Map<String, DatasetRow> rowsByTableName = new HashMap<>();
        for (int colIndex = 1; colIndex <= columnCount; colIndex++) {
            final String tableName = findTableName(resultSetMetaData, colIndex, sqlQuery);
            DatasetRow datasetRow =
                    rowsByTableName.computeIfAbsent(tableName
                                                  , t -> DatasetRow.ofTable(tableName));

            String column = resultSetMetaData.getColumnName(colIndex);
            Object value = resultSet.getObject(colIndex);
            datasetRow.addColumnValue(column, value);
        }
        return rowsByTableName.values();
    }

    private String findTableName(ResultSetMetaData resultSetMetaData, int colIndex, SqlQuery sqlQuery) throws SQLException {
        String tableName = resultSetMetaData.getTableName(colIndex);
        if (!tableName.isEmpty()) {
            return tableName;
        }
        String queryAsString = sqlQuery.getQueryAsString();
        return extractTableNameFrom(queryAsString);
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
