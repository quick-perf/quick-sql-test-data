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

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class DatasetRow {

    private String tableName;

    private TreeMap<String, Object> columnValueByColumnName = new TreeMap<>();

    private DatasetRow(String tableName) {
        this.tableName = tableName;
    }

    public static DatasetRow ofTable(String tableName) {
        return new DatasetRow(tableName);
    }

    protected void addColumnValues(Map<String, Object> columnValues) {
        columnValueByColumnName.putAll(columnValues);
    }

    Set<String> getColumnNames() {
        return columnValueByColumnName.keySet();
    }

    Collection<Object> getColumnValues() {
        return columnValueByColumnName.values();
    }

    Map<String, Object> getColumnValueByColumnName() {
        return new HashMap<>(columnValueByColumnName);
    }

    boolean hasNotNullValueForColumn(String columnName) {
        return columnValueByColumnName.get(columnName) != null;
    }

    void sortColumnsFollowing(List<String> databaseColumnOrders) {
        if (!databaseColumnOrders.isEmpty()) {
            ColumnNamesComparator columnNamesComparator = ColumnNamesComparator.from(databaseColumnOrders);
            TreeMap<String, Object> columnValueByColumnName = new TreeMap<>(columnNamesComparator);
            columnValueByColumnName.putAll(this.columnValueByColumnName);
            this.columnValueByColumnName = columnValueByColumnName;
        }
    }

    boolean mergeWithARowOf(Collection<DatasetRow> datasetRows) {
        Optional<DatasetRow> optionalRowToMergeWith = searchARowToMergeIn(datasetRows);
        if(optionalRowToMergeWith.isPresent()) {
            DatasetRow rowToMergeWith = optionalRowToMergeWith.get();
            rowToMergeWith.addValuesOf(this);
            return true;
        }
        return false;
    }

    private Optional<DatasetRow> searchARowToMergeIn(Collection<DatasetRow> datasetRows) {
        return   datasetRows
                .stream()
                .filter(this::isMergeableWith)
                .findFirst();
    }

    void addValuesOf(DatasetRow datasetRow) {
        TreeMap<String, Object> columnValueByColumnName = datasetRow.columnValueByColumnName;
        for (Map.Entry<String, Object> columnValueOfColumnName : columnValueByColumnName.entrySet()) {
            Object value = columnValueOfColumnName.getValue();
            if(value != null) {
                String columnName = columnValueOfColumnName.getKey();
                this.columnValueByColumnName.put(columnName, value);
            }
        }
    }

    boolean isMergeableWith(DatasetRow otherDatasetRow) {
        if(!this.tableName.equals(otherDatasetRow.getTableName())) {
            return false;
        }
        return sameNotNullColumns(otherDatasetRow);
    }

    String getTableName() {
        return tableName;
    }

    private boolean sameNotNullColumns(DatasetRow otherDatasetRow) {
        TreeMap<String, Object> columnValueByColumnName = otherDatasetRow.columnValueByColumnName;
        for (Map.Entry<String, Object> columnValueOfColumnName : columnValueByColumnName.entrySet()) {
            Object mergeableValue = columnValueOfColumnName.getValue();
            if(mergeableValue != null) {
                String column = columnValueOfColumnName.getKey();
                Object value = this.columnValueByColumnName.get(column);
                if(!mergeableValue.equals(value) && value != null) {
                    return false;
                }
            }
        }

        return true;
    }

    Collection<DatasetRow> extractJoinedRowsFrom(ColumnsMappingGroup columnsMappingGroup) {
        return  columnValueByColumnName
                .entrySet()
                .stream()
                .map(valueForColumn -> {
                    String column = valueForColumn.getKey();
                    Optional<ColumnMappingPart> optionalMappingForColumn =
                            columnsMappingGroup.findMappingForColumn(column);
                    return buildOptionalRowFrom(valueForColumn, optionalMappingForColumn);
                })
                .flatMap(DatasetRow::streamOf)
                .collect(toList());
    }

    private Optional<DatasetRow> buildOptionalRowFrom(Map.Entry<String, Object> valueForColumn, Optional<ColumnMappingPart> optionalMappingForColumn) {
        return optionalMappingForColumn.map(columnMappingPart -> {
            Object value = valueForColumn.getValue();
            DatasetRow joinedRow = new DatasetRow(columnMappingPart.tableName);
            joinedRow.addColumnValue(columnMappingPart.tableColumn, value);
            return joinedRow;
        });
    }

    private static <T> Stream<T> streamOf(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);
    }

    public DatasetRow addColumnValue(String columnName, Object value) {
        columnValueByColumnName.put(columnName, value);
        return this;
    }

    Object getValueOf(String columnName) {
        return columnValueByColumnName.get(columnName);
    }

    void updateTableNameWith(Function<String, String> tableNameFunction) {
        String newTableName = tableNameFunction.apply(tableName);
        tableName = newTableName;
    }

}
