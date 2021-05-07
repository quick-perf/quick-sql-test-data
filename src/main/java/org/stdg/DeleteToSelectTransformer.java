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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.delete.Delete;

import java.util.Optional;

class DeleteToSelectTransformer implements SelectTransformer {

    private Delete deleteStatement;

    DeleteToSelectTransformer(Delete delete) {
        deleteStatement = delete;
    }

    @Override
    public Optional<SqlQuery> toSelect(SqlQuery sqlQuery) {
        String deleteAsString = sqlQuery.getQueryAsString();
        String deleteString = toSelect(deleteAsString);
        SqlQuery deleteQuery = new SqlQuery(deleteString);
        return Optional.of(deleteQuery);
    }

    private String toSelect(String sqlQueryAsString) {
        String tableName = deleteStatement.getTable().getName();

        String whereClauseAsString = findWhereClauseAsString();

        return    " SELECT " + "*"
                + " FROM " + tableName
                + whereClauseAsString;
    }

    private String findWhereClauseAsString() {
        Expression whereExpression = deleteStatement.getWhere();
        String whereClauseAsString = whereExpression == null ? ""
                                     :" WHERE " + whereExpression;
        return whereClauseAsString;
    }

}
