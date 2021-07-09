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

package org.stdg.dbtype;

import org.stdg.ColumnsMappingGroup;
import org.stdg.DatabaseMetadataFinder;
import org.stdg.ReferencedTableSet;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A DatabaseMetadataFinder caching method calls
 */
public class DatabaseMetadataFinderWithCache implements DatabaseMetadataFinder {

    private final DatabaseMetadataFinder delegate;

    private final ConcurrentHashMap<String, Collection<String>> notNullColumnsByTableName = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, List<String>> databaseColumnOrdersByTableName = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, ColumnsMappingGroup> columnsMappingsByTableName = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, ReferencedTableSet> referencedTableSetByTableName = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, List<String>> primaryColumnsByTableName = new ConcurrentHashMap<>();

    public DatabaseMetadataFinderWithCache(DatabaseMetadataFinder delegate) {
        this.delegate = delegate;
    }

    public static DatabaseMetadataFinder buildFrom(DatabaseMetadataFinder databaseMetadataFinder) {
        return new DatabaseMetadataFinderWithCache(databaseMetadataFinder);
    }

    @Override
    public List<String> findDatabaseColumnOrdersOf(String tableName) {
        return databaseColumnOrdersByTableName.computeIfAbsent(tableName, t -> delegate.findDatabaseColumnOrdersOf(tableName));
    }

    @Override
    public ColumnsMappingGroup findColumnsMappingsOf(String tableName) {
        return columnsMappingsByTableName.computeIfAbsent(tableName, t -> delegate.findColumnsMappingsOf(tableName));
    }

    @Override
    public Collection<String> findNotNullColumnsOf(String tableName) {
        return notNullColumnsByTableName.computeIfAbsent(tableName, t -> delegate.findNotNullColumnsOf(tableName));
    }

    @Override
    public ReferencedTableSet findReferencedTablesOf(String tableName) {
        return referencedTableSetByTableName.computeIfAbsent(tableName, t -> delegate.findReferencedTablesOf(tableName));
    }

    @Override
    public List<String> findPrimaryColumnsOf(String tableName) {
        return primaryColumnsByTableName.computeIfAbsent(tableName, t -> delegate.findPrimaryColumnsOf(tableName));
    }

    @Override
    public Function<String, String> getFunctionToHaveMetadataTableName() {
        return delegate.getFunctionToHaveMetadataTableName();
    }

}
