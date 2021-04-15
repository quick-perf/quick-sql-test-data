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

package org.stdg.dbtype;

import org.stdg.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

class BaseReferencedTablesFinder implements ReferencedTablesFinder {

    private final DataSource dataSource;

    private final SqlQuery referencedTableQuery;

    BaseReferencedTablesFinder(DataSource dataSource, SqlQuery referencedTableQuery) {
        this.dataSource = dataSource;
        this.referencedTableQuery =  referencedTableQuery;
    }

    @Override
    public ReferencedTableSet findReferencedTablesOf(String tableName) {

        Collection<ReferencedTable> referencedTables = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement referencedTablesStatement = PreparedStatementBuilder.buildFrom(referencedTableQuery, connection)) {

            referencedTablesStatement.setString(1, tableName);
            ResultSet queryResult = referencedTablesStatement.executeQuery();

            while(queryResult.next()) {
                ReferencedTable referencedTable = buildReferencedTableFrom(queryResult);
                referencedTables.add(referencedTable);
            }

        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
        return new ReferencedTableSet(referencedTables);
    }

    private ReferencedTable buildReferencedTableFrom(ResultSet queryResult) throws SQLException {
        String resultTableName = queryResult.getString(1);
        String referencedTableName = queryResult.getString(2);
        int level = queryResult.getInt(3);
        return new ReferencedTable(resultTableName
                                 , referencedTableName
                                 , level);
    }

}
