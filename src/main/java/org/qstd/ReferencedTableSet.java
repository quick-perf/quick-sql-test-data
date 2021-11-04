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

import java.util.Collection;

import static java.util.Collections.emptyList;

public class ReferencedTableSet {

    public static final ReferencedTableSet NONE = new ReferencedTableSet(emptyList());

    private final Collection<ReferencedTable> referencedTablesOfTable;

    public ReferencedTableSet(Collection<ReferencedTable> referencedTablesOfTable) {
        this.referencedTablesOfTable = referencedTablesOfTable;
    }

    boolean referencesTable(String tableName) {
        for (ReferencedTable referencedTable : referencedTablesOfTable) {
            if (referencedTable.references(tableName)) {
                return true;
            }
        }
        return false;
    }

}
