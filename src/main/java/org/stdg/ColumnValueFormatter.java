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

import org.stdg.dbtype.DatabaseType;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetTime;
import java.util.Calendar;

class ColumnValueFormatter {

    static final ColumnValueFormatter INSTANCE = new ColumnValueFormatter();

    private ColumnValueFormatter() { }

    String formatColumnValue(Object columnValue, DatabaseType dbType) {
        if(columnValue == null) {
            return "NULL";
        } else if(DatabaseType.ORACLE.equals(dbType)
               && columnValue instanceof Timestamp) {
            Timestamp timeStamp = (Timestamp) columnValue;
            return buildOracleToDateFunctionFor(timeStamp);
        } else if(DatabaseType.ORACLE.equals(dbType)
               && isOracleSqlTimestamp(columnValue)) {
            return buildOracleToTimeStampFunctionFor(columnValue);
        } else if (columnValue instanceof String
                || columnValue instanceof java.sql.Date
                || columnValue instanceof Timestamp
                || columnValue instanceof Time
                || columnValue instanceof OffsetTime
                || isTimestampWithTimeZoneH2Type(columnValue)
                || isMicrosoftDateTimeOffset(columnValue)) {
            String stringColumnValue = columnValue.toString();
            return "'" + stringColumnValue + "'";
        }
        return columnValue.toString();
    }

    private boolean isMicrosoftDateTimeOffset(Object columnValue) {
        Class<?> columnValueClass = columnValue.getClass();
        String classCanonicalName = columnValueClass.getCanonicalName();
        return "microsoft.sql.DateTimeOffset".equals(classCanonicalName);
    }

    private String buildOracleToDateFunctionFor(Timestamp timeStamp) {
        //https://stackoverflow.com/questions/9180014/using-oracle-to-date-function-for-date-string-with-milliseconds
        // "An Oracle DATE does not store times with more precision than a second."
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(timeStamp);
        int monthNumber = calendar.get(Calendar.MONTH) + 1;
        int secondNumber = calendar.get(Calendar.SECOND);
        String toDateString = calendar.get(Calendar.YEAR)
                            + "-" + (monthNumber < 10 ? "0" : "") + monthNumber
                            + "-" + calendar.get(Calendar.DAY_OF_MONTH)
                            + "-" + calendar.get(Calendar.HOUR_OF_DAY)
                            + "-" + calendar.get(Calendar.MINUTE)
                            + "-" + (secondNumber < 10 ? "0" : "") + secondNumber;
        return "TO_DATE('" + toDateString + "', 'yyyy-mm-dd-HH24-mi-ss')";
    }

    private boolean isOracleSqlTimestamp(Object columnValue) {
        Class<?> columnValueClass = columnValue.getClass();
        String classCanonicalName = columnValueClass.getCanonicalName();
        return classCanonicalName.equals("oracle.sql.TIMESTAMP");
    }

    private String buildOracleToTimeStampFunctionFor(Object columnValue) {
        String oracleTimeStampAsString = columnValue.toString();
        String aDateWithMsLessThan100 = "2012-09-17 19:56:47.10";
        boolean dateHasMsLessThan100 = oracleTimeStampAsString.length() == aDateWithMsLessThan100.length();
        String dateForTimeStampCreation = dateHasMsLessThan100 ? oracleTimeStampAsString + "0" : oracleTimeStampAsString;
        return "TO_TIMESTAMP('" + dateForTimeStampCreation
                           + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
    }

    private boolean isTimestampWithTimeZoneH2Type(Object columnValue) {
        Class<?> columnValueClass = columnValue.getClass();
        String classCanonicalName = columnValueClass.getCanonicalName();
        return classCanonicalName.equals("org.h2.api.TimestampWithTimeZone");
    }

}
