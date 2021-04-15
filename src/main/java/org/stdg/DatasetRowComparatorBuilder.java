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

import java.util.Comparator;
import java.util.List;

class DatasetRowComparatorBuilder {

    private DatasetRowComparatorBuilder() { }

    static Comparator<DatasetRow> buildFrom(DatabaseMetadataFinder databaseMetadataFinder) {
        ComparatorOnTableDependencies comparatorOnTableDependencies = new ComparatorOnTableDependencies(databaseMetadataFinder);
        ComparatorOnPrimaryKey comparatorOnPrimaryKey = new ComparatorOnPrimaryKey(databaseMetadataFinder);
        return comparatorOnTableDependencies.thenComparing(comparatorOnPrimaryKey);
    }

    private static class ComparatorOnPrimaryKey implements Comparator<DatasetRow> {

        private final PrimaryKeyColumnsFinder primaryKeyColumnsFinder;

        ComparatorOnPrimaryKey(PrimaryKeyColumnsFinder primaryKeyColumnsFinder) {
            this.primaryKeyColumnsFinder = primaryKeyColumnsFinder;
        }

        @Override
        public int compare(DatasetRow datasetRow1, DatasetRow datasetRow2) {

            if(!sameTableNames(datasetRow1, datasetRow2)) {
                return 0;
            }

            String tableName = datasetRow1.getTableName();
            List<String> primaryColumns = primaryKeyColumnsFinder.findPrimaryColumnsOf(tableName);

            String primaryKey1 = computePrimaryKeyAsString(datasetRow1, primaryColumns);
            String primaryKey2 = computePrimaryKeyAsString(datasetRow2, primaryColumns);

            return primaryKey1.compareTo(primaryKey2);

        }

        private boolean sameTableNames(DatasetRow datasetRow1, DatasetRow datasetRow2) {
            return datasetRow1.getTableName().equals(datasetRow2.getTableName());
        }

        private String computePrimaryKeyAsString(DatasetRow datasetRow1, List<String> primaryColumns) {
            StringBuilder resultAsStringBuilder = new StringBuilder();
            for (String primaryColumn : primaryColumns) {
                Object primaryKeyValue = datasetRow1.getValueOf(primaryColumn);
                String primaryKeyValueAsString = primaryKeyValue.toString();
                resultAsStringBuilder.append(primaryKeyValueAsString);
            }
            return resultAsStringBuilder.toString();
        }

    }

    private static class ComparatorOnTableDependencies implements Comparator<DatasetRow> {

        private final ReferencedTablesFinder referencedTablesFinder;

        public ComparatorOnTableDependencies(ReferencedTablesFinder referencedTablesFinder) {
            this.referencedTablesFinder = referencedTablesFinder;
        }

        @Override
        public int compare(DatasetRow datasetRow1, DatasetRow datasetRow2) {

            String tableName1 = datasetRow1.getTableName();
            String tableName2 = datasetRow2.getTableName();

            ReferencedTableSet referencedTableSetOfTable1
                    = referencedTablesFinder.findReferencedTablesOf(tableName1);
            if(referencedTableSetOfTable1.referencesTable(tableName2)) {
                return 1;
            }

            ReferencedTableSet referencedTableSetOfTable2
                    = referencedTablesFinder.findReferencedTablesOf(tableName2);
            if(referencedTableSetOfTable2.referencesTable(tableName1)) {
                return -1;
            }

            return 0;

        }

    }
}
