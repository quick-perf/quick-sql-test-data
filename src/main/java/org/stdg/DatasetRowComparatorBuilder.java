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
        ComparatorOnTableName comparatorOnTableName = new ComparatorOnTableName();
        return comparatorOnTableDependencies.thenComparing(comparatorOnPrimaryKey)
                                            .thenComparing(comparatorOnTableName);
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
            List<String> primaryKeyColumns = primaryKeyColumnsFinder.findPrimaryColumnsOf(tableName);

            for (String primaryKeyColumn : primaryKeyColumns) {
                int intComparison = compareIntPkValues(primaryKeyColumn, datasetRow1, datasetRow2);
                if(intComparison != 0) {
                    return intComparison;
                }
            }

            return 0;

        }

        private boolean sameTableNames(DatasetRow datasetRow1, DatasetRow datasetRow2) {
            return datasetRow1.getTableName().equals(datasetRow2.getTableName());
        }

        private int compareIntPkValues(String primaryKeyColumn, DatasetRow datasetRow1, DatasetRow datasetRow2) {
            Object pkValue1 = datasetRow1.getValueOf(primaryKeyColumn);
            Object pkValue2 = datasetRow2.getValueOf(primaryKeyColumn);
            boolean integerPrimaryKey = pkValue1 instanceof Integer
                                     || pkValue1 instanceof Long;
            if(integerPrimaryKey) {
                Number pkValue1AsNumber = (Number) pkValue1;
                Number pkValue2AsNumber = (Number) pkValue2;
                long pkValue1AsLong = pkValue1AsNumber.longValue();
                long pkValue2AsLong = pkValue2AsNumber.longValue();
                return Long.compare(pkValue1AsLong, pkValue2AsLong );
            }
            return 0;
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

    private static class ComparatorOnTableName implements Comparator<DatasetRow> {

        @Override
        public int compare(DatasetRow row1, DatasetRow row2) {
            String row1TableName = row1.getTableName();
            String row2TableName = row2.getTableName();
            return row1TableName.compareTo(row2TableName);
        }

    }

}
