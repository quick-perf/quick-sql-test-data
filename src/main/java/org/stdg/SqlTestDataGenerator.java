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

import org.stdg.dbtype.DatabaseMetadataFinderFactory;
import org.stdg.dbtype.DatabaseMetadataFinderWithCache;
import org.stdg.dbtype.DatabaseType;
import org.stdg.dbtype.DatabaseUrlFinder;

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
public class SqlTestDataGenerator {

    private final DatasetRowsFinder datasetRowsFinder;

    private SqlTestDataGenerator(DatasetRowsFinder datasetRowsFinder) {
        this.datasetRowsFinder = datasetRowsFinder;
    }

    /**
     * Factory method to build an instance of <code>org.stdg.SqlTestDataGenerator</code> from a data source.
     * The retrieval of database metadata, such as not null columns or primary keys, is cached for each table.
     * @param dataSource A data source
     * @return An instance of <code>org.stdg.SqlTestDataGenerator</code>
     */
    public static SqlTestDataGenerator buildFrom(DataSource dataSource) {
        String dbUrl = DatabaseUrlFinder.INSTANCE.findDbUrlFrom(dataSource);
        DatabaseType dbType = DatabaseType.findFromDbUrl(dbUrl);
        DatabaseMetadataFinder databaseMetadataFinder = DatabaseMetadataFinderFactory.createFrom(dataSource, dbType);
        DatabaseMetadataFinder databaseMetadataFinderWithCache = DatabaseMetadataFinderWithCache.buildFrom(databaseMetadataFinder);
        return buildFrom(dataSource, databaseMetadataFinderWithCache);
    }

    public static SqlTestDataGenerator buildFrom(DataSource dataSource, DatabaseMetadataFinder databaseMetadataFinder) {
        DatasetRowsFinder datasetRowsFinder = new DatasetRowsFinder(dataSource, databaseMetadataFinder);
        return new SqlTestDataGenerator(datasetRowsFinder);
    }

    public String generateInsertScriptFor(String sqlQuery) {
        return generateInsertScriptFor(sqlQuery, emptyList());
    }

    public String generateInsertScriptFor(String query, List<Object> parameters) {
        List<SqlQuery> sqlQueries = singletonList(new SqlQuery(query, parameters));
        return generateInsertScriptFor(sqlQueries);
    }

    public String generateInsertScriptFor(List<SqlQuery> sqlQueries) {
        List<DatasetRow> datasetRows = datasetRowsFinder.findDatasetRowsFrom(sqlQueries);
        return InsertStatementsGenerator.INSTANCE.generateInsertScriptFor(datasetRows);
    }

    public String generateInsertScriptFor(String... sqlQueries) {
        List<SqlQuery> queries = stream(sqlQueries)
                                .map(SqlQuery::new)
                                .collect(toList());
        return generateInsertScriptFor(queries);
    }

    public List<String> generateInsertListFor(DatasetRow datasetRow) {
        SqlQuery sqlQuery = SqlQuery.buildFromRow(datasetRow);
        return generateInsertListFor(sqlQuery.toString());
    }

    public List<String> generateInsertListFor(String... sqlQueries) {
        List<SqlQuery> sqlQueryObjects = stream(sqlQueries)
                                        .map(SqlQuery::new)
                                        .collect(toList());
        List<DatasetRow> datasetRows = datasetRowsFinder.findDatasetRowsFrom(sqlQueryObjects);
        return InsertStatementsGenerator.INSTANCE.generateInsertStatementsFor(datasetRows);
    }

}
