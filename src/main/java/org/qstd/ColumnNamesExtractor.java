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

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class ColumnNamesExtractor {

    static final ColumnNamesExtractor INSTANCE = new ColumnNamesExtractor();

    private ColumnNamesExtractor() {
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

    Set<String> findColumnNamesOf(Expression expression) {
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

}
