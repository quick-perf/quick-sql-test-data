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

package org.qstd;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

class UpdateToSelectTransformer implements SelectTransformer {

    private Update updateStatement;

    UpdateToSelectTransformer(Update update) {
        updateStatement = update;
    }

    @Override
    public Optional<SqlQuery> toSelect(SqlQuery sqlQuery)  {
        String tableName = updateStatement.getTable().getName();
        String selectAsString = " SELECT " + findSelectedColumnsSeparatedWithCommas()
                              + " FROM " + tableName
                              + findWhereClauseIfExists();
        List<Object> parameters = sqlQuery.getParameters();
        SqlQuery selectQuery = new SqlQuery(selectAsString, parameters);
        return Optional.of(selectQuery);
    }

    private String findSelectedColumnsSeparatedWithCommas() {
        Collection<String> columns = findColumnsToSelect();
        return String.join(", ", columns);
    }

    private Collection<String> findColumnsToSelect() {
        Collection<String> columnNames = findUpdatedColumnNames();
        Collection<String> columnsToSelect = new ArrayList<>(columnNames);
        Collection<String> whereColumnNames = findWhereColumnNames();
        columnsToSelect.addAll(whereColumnNames);
        return columnsToSelect;
    }

    private List<String> findUpdatedColumnNames() {
        return   updateStatement
                .getColumns()
                .stream()
                .map(Column::getColumnName)
                .collect(toList());
    }

    private String findWhereClauseIfExists() {
        Expression whereExpression = updateStatement.getWhere();
        return   whereExpression == null ? ""
                :" WHERE " + whereExpression;
    }

    private Collection<String> findWhereColumnNames() {
        Expression whereExpression = updateStatement.getWhere();
        return ColumnNamesExtractor.INSTANCE.findColumnNamesOf(whereExpression);
    }

}
