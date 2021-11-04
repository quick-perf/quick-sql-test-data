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

public class ColumnsMapping {

    private final ColumnMappingPart columnMappingPart1;

    private final ColumnMappingPart columnMappingPart2;

    public ColumnsMapping(ColumnMappingPart columnMappingPart1, ColumnMappingPart columnMappingPart2) {
        this.columnMappingPart1 = columnMappingPart1;
        this.columnMappingPart2 = columnMappingPart2;
    }

    boolean hasMappingForColumn(String columnName) {
        return columnMappingPart1.hasColumn(columnName);
    }

    ColumnMappingPart getMapping() {
        return columnMappingPart2;
    }

}
