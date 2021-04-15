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

import org.stdg.DatabaseMetadataFinder;

import javax.sql.DataSource;

import static org.stdg.dbtype.DatabaseType.*;

/**
 * Factory to create an instance of {@link org.stdg.DatabaseMetadataFinder}.
 */
public class DatabaseMetadataFinderFactory {

    private DatabaseMetadataFinderFactory() { }

    /**
     * Creates a DatabaseMetadataFinder
     * @param dataSource A data source
     * @param dbType A database type
     * @return An instance of DatabaseMetadataFinder
     */
    public static DatabaseMetadataFinder createFrom(DataSource dataSource, DatabaseType dbType) {

        if(dbType.equals(H2)) {
            return new H2MetadataFinder(dataSource);
        }

        if(dbType.equals(HSQLDB)) {
            return new HsqlDbMetadataFinder(dataSource);
        }

        if(dbType.equals(POSTGRE_SQL)) {
            return new PostgreSqlMetadataFinder(dataSource);
        }

        return new DefaultDatabaseMetadataFinder(dataSource);

    }

}
