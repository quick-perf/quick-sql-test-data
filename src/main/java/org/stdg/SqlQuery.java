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
import net.sf.jsqlparser.statement.update.Update;

import java.util.*;

import static java.util.stream.Collectors.joining;

public class SqlQuery {

    private final String queryAsString;

    private final List<Object> parameters;

    private static final SqlQuery NONE = new SqlQuery("");

    public SqlQuery(String queryAsString) {
        this.queryAsString = queryAsString;
        this.parameters = Collections.emptyList();
    }

    public SqlQuery(String queryAsString, List<Object> parameters) {
        this.queryAsString = queryAsString;
        this.parameters = parameters;
    }

    SqlQuery transformToSelectQuery() {
        Statement parsedStatement = parse(queryAsString);
        if(parsedStatement instanceof Select) {
            return this;
        }
        if(parsedStatement instanceof Update) {
            Update update = (Update) parsedStatement;
            SelectTransformer selectTransformer = new SelectTransformer(update);
            String selectQueryAsString = selectTransformer.transformToSelect();
            return new SqlQuery(selectQueryAsString, parameters);
        }
        return NONE;
    }

   private Statement parse(String sqlQuery) {
        try {
            return CCJSqlParserUtil.parse(sqlQuery);
        } catch (JSQLParserException e) {
            e.printStackTrace();
            return null;
        }
    }

    static SqlQuery buildFromRow(DatasetRow rowToSearch) {
        Set<String> columnNames = rowToSearch.getColumnNames();
        return buildFromRow(columnNames, rowToSearch);

    }

    static SqlQuery buildFromRow(Collection<String> columnNamesToSearch, DatasetRow rowToSearch) {
            Map<String, Object> valuesToMatch = rowToSearch.getColumnValueByColumnName();
            String whereConditions =
                     valuesToMatch.entrySet()
                    .stream()
                    .map(entry -> {
                        String columnName = entry.getKey();
                        return columnName
                                + (entry.getValue() == null
                                   ? " IS NULL"
                                   : "=" + ColumnValueFormatter.INSTANCE.formatColumnValue(entry.getValue()));
                    })
                    .collect(joining(" AND "));
            String queryAsString =
                              "SELECT "
                            + String.join(", ", columnNamesToSearch)
                            + " FROM " + rowToSearch.getTableName()
                            + " WHERE " + whereConditions;
            return new SqlQuery(queryAsString);

    }

    @Override
    public String toString() {
        return getQueryAsString();
    }

    String getQueryAsString() {
        return queryAsString;
    }

    List<Object> getParameters() {
        return parameters;
    }

}
