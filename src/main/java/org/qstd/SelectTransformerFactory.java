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

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import java.util.Optional;

class SelectTransformerFactory {

    private SelectTransformerFactory() {
    }

    private static final SelectTransformer SELECT_TO_SELECT_TRANSFORMER =

            new SelectTransformer() {
                @Override
                public Optional<SqlQuery> toSelect(SqlQuery sqlQuery) {
                    return Optional.of(sqlQuery);
                }
            };

    static SelectTransformer createSelectTransformer(SqlQuery sqlQuery) {

        String sqlQueryAsString = sqlQuery.getQueryAsString();
        Statement statement = parse(sqlQueryAsString);

        if(statement instanceof Select) {
            return SELECT_TO_SELECT_TRANSFORMER;
        }
        if(statement instanceof Update) {
            Update update = (Update) statement;
            return new UpdateToSelectTransformer(update);
        }
        if(statement instanceof Delete) {
            Delete delete = (Delete) statement;
            return new DeleteToSelectTransformer(delete);
        }

        return SelectTransformer.NO_SELECT_TRANSFORMER;
    }

    private static Statement parse(String sqlQuery) {
        try {
            return CCJSqlParserUtil.parse(sqlQuery);
        } catch (JSQLParserException e) {
            e.printStackTrace();
            return null;
        }
    }

}
