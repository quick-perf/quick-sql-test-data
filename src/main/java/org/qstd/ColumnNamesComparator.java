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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ColumnNamesComparator implements Comparator<String> {

    private final Map<String, Integer> positionByColumnName;

    private ColumnNamesComparator(Map<String, Integer> positionByColumnName) {
        this.positionByColumnName = positionByColumnName;
    }

    public static ColumnNamesComparator from(List<String> orderedColumns) {
        Map<String, Integer> positionByColumnName = buildIndexByColumnName(orderedColumns);
        return new ColumnNamesComparator(positionByColumnName);
    }

    private static Map<String, Integer> buildIndexByColumnName(List<String> orderedColumns) {
        final Map<String, Integer> positionByColumnName = new HashMap<>();
        for (int i = 0; i < orderedColumns.size(); i++) {
            positionByColumnName.put(orderedColumns.get(i), i + 1);
        }
        return positionByColumnName;
    }

    @Override
    public int compare(String colName1, String colName2) {
        return findPositionOf(colName1) - findPositionOf(colName2);
    }

    private int findPositionOf(String colName1) {
        return positionByColumnName.get(colName1);
    }

}
