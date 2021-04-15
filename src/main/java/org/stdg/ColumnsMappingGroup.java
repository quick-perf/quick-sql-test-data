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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class ColumnsMappingGroup {

    public static final ColumnsMappingGroup NO_MAPPING = new ColumnsMappingGroup(Collections.emptyList());

    private final Collection<ColumnsMapping> columnsMappings;

    public ColumnsMappingGroup(Collection<ColumnsMapping> columnsMappings) {
        this.columnsMappings = new ArrayList<>(columnsMappings);
    }

    Optional<ColumnMappingPart> findMappingForColumn(String columnName) {
        return   columnsMappings
                .stream()
                .filter(columnsMapping -> columnsMapping.hasMappingForColumn(columnName))
                .map(ColumnsMapping::getMapping)
                .findFirst();
    }

}
