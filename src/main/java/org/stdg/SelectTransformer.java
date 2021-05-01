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

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;

import java.util.*;

import static java.util.stream.Collectors.toList;

class SelectTransformer {

    private final Update updateStatement;

    SelectTransformer(Update update) {
        updateStatement = update;
    }

    String transformToSelect() {
        String tableName = updateStatement.getTable().getName();
        return    "SELECT " + findSelectedColumnsSeparatedWithCommas()
                + " FROM " + tableName
                + findWhereClauseIfExists();
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
        return findColumnNamesOf(whereExpression);
    }

    private Set<String> findColumnNamesOf(Expression expression) {
        Set<String> columnNames = new HashSet<>();
        if (expression instanceof BinaryExpression) {
            // AndExpression, OrExpression, LikeExpression, ...
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            Collection<String> leftRightColumnNames = extractColumnNamesOf(binaryExpression);
            columnNames.addAll(leftRightColumnNames);
        } else if (expression != null) {
            // Column names
            ColumnExpressionVisitor columnExpressionVisitor = new ColumnExpressionVisitor();
            expression.accept(columnExpressionVisitor);
            String visitedColumnName = columnExpressionVisitor.getVisitedColumnName();
            if(visitedColumnName != null) {
                columnNames.add(visitedColumnName);
            }
        }
        return columnNames;
    }

    private Collection<String> extractColumnNamesOf(BinaryExpression binaryExpression) {
        Collection<String> leftRightColumnNames = new ArrayList<>();
        Collection<String> leftColumnNames = findColumnNamesOf(binaryExpression.getLeftExpression());
        Collection<String> rightColumnNames = findColumnNamesOf(binaryExpression.getRightExpression());
        leftRightColumnNames.addAll(leftColumnNames);
        leftRightColumnNames.addAll(rightColumnNames);
        return leftRightColumnNames;
    }

    private static class ColumnExpressionVisitor extends ExpressionVisitorAdapter {

        private String visitedColumnName;

        @Override
        public void visit(Column column) {
            this.visitedColumnName = column.getColumnName();
        }

        String getVisitedColumnName() {
            return visitedColumnName;
        }
    }

}
