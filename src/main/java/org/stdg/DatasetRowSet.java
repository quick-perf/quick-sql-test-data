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

import javax.sql.DataSource;
import java.util.*;

class DatasetRowSet {

    private final MissingNotNullColumnsFinder missingNotNullColumnsFinder;

    private final DatabaseMetadataFinder databaseMetadataFinder;

    private final Collection<DatasetRow> datasetRows = new ArrayDeque<>();

    DatasetRowSet(DataSource dataSource, DatabaseMetadataFinder databaseMetadataFinder) {
        this.databaseMetadataFinder = databaseMetadataFinder;
        this.missingNotNullColumnsFinder = new MissingNotNullColumnsFinder(dataSource, databaseMetadataFinder);
    }

    void add(DatasetRow datasetRow) {

        boolean rowIsMerged = datasetRow.mergeWithARowOf(datasetRows);

        if (!rowIsMerged) {
            Map<String, Object> missingNotNullColumns =
                    missingNotNullColumnsFinder.findMissingNoNullColumnsOf(datasetRow);
            datasetRow.addColumnValues(missingNotNullColumns);

            datasetRows.add(datasetRow);

            Collection<DatasetRow> joinedRows = findJoinedRowsOf(datasetRow);
            for (DatasetRow joinRow : joinedRows) {
                add(joinRow);
            }
        }

    }

    private Collection<DatasetRow> findJoinedRowsOf(DatasetRow datasetRow) {
        String tableName = datasetRow.getTableName();
        ColumnsMappingGroup columnsMappingGroup =
                databaseMetadataFinder.findColumnsMappingsOf(tableName);
        return datasetRow.extractJoinedRowsFrom(columnsMappingGroup);
    }

    List<DatasetRow> sort() {
        sortColumnsFollowingDatabaseDeclaration(datasetRows);
        return sortRows();
    }

    private void sortColumnsFollowingDatabaseDeclaration(Collection<DatasetRow> allRows) {
        for (DatasetRow datasetRow : allRows) {
            String tableName = datasetRow.getTableName();
            List<String> databaseColumnOrders = databaseMetadataFinder.findDatabaseColumnOrdersOf(tableName);
            datasetRow.sortColumnsFollowing(databaseColumnOrders);
        }
    }

    private List<DatasetRow> sortRows() {
        List<DatasetRow> rowsAsList = new ArrayList<>(datasetRows);
        Comparator<DatasetRow> datasetRowComparator =
                DatasetRowComparatorBuilder.buildFrom(databaseMetadataFinder);
        rowsAsList.sort(datasetRowComparator);
        return rowsAsList;
    }

}
