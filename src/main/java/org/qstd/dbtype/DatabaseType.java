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

package org.qstd.dbtype;

import static java.util.Arrays.stream;

/**
 * Database type
 */
public enum DatabaseType {

    /**H2*/
    H2("jdbc:h2")
    ,/**HSQLDB*/
    HSQLDB("jdbc:hsqldb")
    ,/**MariaDB*/
    MARIA_DB("jdbc:mariadb")
    ,/**Microsoft SQL Server*/
    MICROSOFT_SQL_SERVER("jdbc:sqlserver")
    ,/**MySQL*/
    MY_SQL("jdbc:mysql")
    ,/**Oracle*/
    ORACLE("jdbc:oracle")
    ,/**PostgreSQL*/
    POSTGRE_SQL("jdbc:postgresql")
    ,/**Other database type*/
    OTHER("jdbc:");

    private final String jdbcUrlStart;

    DatabaseType(String jdbcUrlStart) {
        this.jdbcUrlStart = jdbcUrlStart;
    }

    /**
     * Find the database type from the JDBC URL
     * @param jdbcUrl A JDBC URL
     * @return The database type
     */
    public static DatabaseType findFromDbUrl(String jdbcUrl) {
        DatabaseType[] databaseTypes = DatabaseType.values();
        return   stream(databaseTypes)
                .filter(dbType -> dbType.accept(jdbcUrl))
                .findFirst()
                .get();
    }

    private boolean accept(String jdbcUrl) {
        return jdbcUrl.startsWith(jdbcUrlStart);
    }

}
