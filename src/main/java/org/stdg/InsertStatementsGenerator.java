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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

class InsertStatementsGenerator {

    public static final InsertStatementsGenerator INSTANCE = new InsertStatementsGenerator();

    private InsertStatementsGenerator() { }

    String generateInsertScriptFor(List<DatasetRow> datasetRows) {
        return   datasetRows
                .stream()
                .map(this::generateInsertStatementFrom)
                .collect(joiningScriptLines()) + ";";
    }

    private String generateInsertStatementFrom(DatasetRow datasetRow) {
        String tableName = datasetRow.getTableName();
        Set<String> columnNames = datasetRow.getColumnNames();
        Collection<Object> columnValues = datasetRow.getColumnValues();
        return  "INSERT INTO " + tableName + "(" + formatColumnNames(columnNames) + ")"
              + " VALUES(" + formatColumnValues(columnValues) + ")";
    }

    private Collector<CharSequence, ?, String> joiningScriptLines() {
        return joining(";" + System.lineSeparator());
    }

    private String formatColumnNames(Set<String> columnNames) {
        return String.join(", ", columnNames);
    }

    private String formatColumnValues(Collection<Object> columnValues) {
        return  columnValues
               .stream()
               .map(ColumnValueFormatter.INSTANCE::formatColumnValue)
               .collect(joining(", "));
    }

    List<String> generateInsertStatementsFor(List<DatasetRow> datasetRows) {
        return   datasetRows
                .stream()
                .map(this::generateInsertStatementFrom)
                .collect(toList());
    }

}
