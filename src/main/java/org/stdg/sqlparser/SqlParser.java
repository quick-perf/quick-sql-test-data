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

package org.stdg.sqlparser;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;

public class SqlParser {

    public static Update parseUpdateStatement(String insertQuery) {
        return (Update) parseFrom(insertQuery);
    }

    public static Statement parseFrom(String sqlQuery) {
        try {
            return CCJSqlParserUtil.parse(sqlQuery);
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return null;
    }

} 