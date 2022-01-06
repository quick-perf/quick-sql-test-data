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

public class ColumnMappingPart {

    final String tableName;
    final String tableColumn;
    private final String tableSchema;

    public ColumnMappingPart(String tableSchema, String tableName, String tableColumn) {
        this.tableSchema = tableSchema;
        this.tableName = tableName;
        this.tableColumn = tableColumn;
    }

    boolean hasColumn(String columnName) {
        return tableColumn.equals(columnName);
    }

}
