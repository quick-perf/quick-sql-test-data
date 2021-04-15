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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Class helping to find database URL
 */
public class DatabaseUrlFinder {

    public static final DatabaseUrlFinder INSTANCE = new DatabaseUrlFinder();

    /**
     * Find the database URL from a data source
     * @param dataSource A data source
     * @return The database URL
     */
    public static String findDbUrlFrom(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            return metaData.getURL().toLowerCase();
        } catch (SQLException sqlException) {
            throw new IllegalStateException(sqlException);
        }
    }

}
