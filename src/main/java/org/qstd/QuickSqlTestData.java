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

import org.qstd.dbtype.DatabaseMetadataFinderFactory;
import org.qstd.dbtype.DatabaseMetadataFinderWithCache;
import org.qstd.dbtype.DatabaseType;
import org.qstd.dbtype.DatabaseUrlFinder;

import javax.sql.DataSource;
import java.util.List;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * Class allowing to ease the generation of datasets to test SQL queries.
 * Methods produce INSERT statements taking account of database integrity constraints.
 *
 * @see SqlQuery
 * @see DatasetRow
 * @see DatabaseMetadataFinder
 * @see DatabaseMetadataFinderFactory
 * @see DatabaseMetadataFinderWithCache
 */
public class QuickSqlTestData {

    private final DatasetRowsGenerator datasetRowsGenerator;

    private final DatabaseType dbType;

    private InsertStatementsGenerator insertStatementGenerator;

    private QuickSqlTestData(DatasetRowsGenerator datasetRowsGenerator, DatabaseType dbType) {
        this.datasetRowsGenerator = datasetRowsGenerator;
        this.dbType = dbType;
        insertStatementGenerator = new InsertStatementsGenerator(dbType);
    }

    /**
     * Factory method to build an instance of <code>org.qstd.QuickSqlTestData</code> from a data source.
     * The retrieval of database metadata, such as not null columns or primary keys, is cached for each table.
     * @param dataSource A data source
     * @return An instance of <code>org.qstd.QuickSqlTestData</code>
     */
    public static QuickSqlTestData buildFrom(DataSource dataSource) {
        String dbUrl = DatabaseUrlFinder.INSTANCE.findDbUrlFrom(dataSource);
        DatabaseType dbType = DatabaseType.findFromDbUrl(dbUrl);
        DatabaseMetadataFinder databaseMetadataFinder = DatabaseMetadataFinderFactory.createDatabaseMetadataFinderFrom(dataSource, dbType);
        DatabaseMetadataFinder databaseMetadataFinderWithCache = DatabaseMetadataFinderWithCache.buildFrom(databaseMetadataFinder);
        return buildFrom(dataSource, dbType, databaseMetadataFinderWithCache);
    }

    public static QuickSqlTestData buildFrom(DataSource dataSource, DatabaseType dbType, DatabaseMetadataFinder databaseMetadataFinder) {
        DatasetRowsGenerator datasetRowsGenerator = new DatasetRowsGenerator(dataSource, dbType, databaseMetadataFinder);
        return new QuickSqlTestData(datasetRowsGenerator, dbType);
    }

    public String generateInsertScriptFor(String sqlQuery) {
        return generateInsertScriptFor(sqlQuery, emptyList());
    }

    public String generateInsertScriptFor(String query, List<Object> parameters) {
        List<SqlQuery> sqlQueries = singletonList(new SqlQuery(query, parameters));
        return generateInsertScriptFor(sqlQueries);
    }

    public String generateInsertScriptFor(List<SqlQuery> sqlQueries) {
        List<DatasetRow> datasetRows = datasetRowsGenerator.generateDatasetRowsFor(sqlQueries);
        return insertStatementGenerator.generateInsertScriptFor(datasetRows);
    }

    public String generateInsertScriptFor(String... sqlQueries) {
        List<SqlQuery> queries = stream(sqlQueries)
                                .map(SqlQuery::new)
                                .collect(toList());
        return generateInsertScriptFor(queries);
    }

    public List<String> generateInsertListFor(DatasetRow datasetRow) {
        SqlQuery sqlQuery = SqlQuery.buildFromRow(datasetRow, dbType);
        return generateInsertListFor(sqlQuery.toString());
    }

    public List<String> generateInsertListFor(String... sqlQueries) {
        List<SqlQuery> sqlQueryObjects = stream(sqlQueries)
                                        .map(SqlQuery::new)
                                        .collect(toList());
        List<DatasetRow> datasetRows = datasetRowsGenerator.generateDatasetRowsFor(sqlQueryObjects);
        return insertStatementGenerator.generateInsertStatementsFor(datasetRows);
    }

}
