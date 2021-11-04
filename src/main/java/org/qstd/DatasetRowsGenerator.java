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

import org.qstd.dbtype.DatabaseType;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.List;

class DatasetRowsGenerator {

    private final DataSource dataSource;

    private final DatabaseType dbType;

    private final DatabaseMetadataFinder databaseMetadataFinder;

    private final DatasetRowsFinder datasetRowsFinder;

    DatasetRowsGenerator(DataSource dataSource
                       , DatabaseType dbType
                       , DatabaseMetadataFinder databaseMetadataFinder) {
        this.dataSource = dataSource;
        this.dbType = dbType;
        this.databaseMetadataFinder = databaseMetadataFinder;
        this.datasetRowsFinder = new DatasetRowsFinder(dataSource);
    }

    List<DatasetRow> generateDatasetRowsFor(List<SqlQuery> sqlQueries) {
        DatasetRowSet datasetRowSet = new DatasetRowSet(dataSource, dbType, databaseMetadataFinder);
        for (SqlQuery sqlQuery : sqlQueries) {
            Collection<DatasetRow> datasetRows = datasetRowsFinder.findDatasetRowsOf(sqlQuery);
            datasetRowSet.add(datasetRows);
        }
        return datasetRowSet.sort();
    }

}
