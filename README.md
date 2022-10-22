# Quick SQL test data

  <a href="https://search.maven.org/artifact/org.quickperf/quick-sql-test-data">
    <img src="https://maven-badges.herokuapp.com/maven-central/org.quickperf/quick-sql-test-data/badge.svg"
         alt="Maven Central">
  </a>
  &nbsp;&nbsp;
  <a href="https://github.com/quick-perf/quick-sql-test-data/blob/master/LICENSE.txt">
    <img src="https://img.shields.io/badge/license-Apache2-blue.svg"
         alt = "License">
  </a>
  &nbsp;&nbsp;
  <a href="https://github.com/quick-perf/quick-sql-test-data/actions?query=workflow%3ACI">
    <img src="https://img.shields.io/github/workflow/status/quick-perf/quick-sql-test-data/CI"
         alt = "Build Status">
  </a>
  &nbsp;
  <a href="https://codecov.io/gh/quick-perf/quick-sql-test-data">
    <img src="https://codecov.io/gh/quick-perf/quick-sql-test-data/branch/main/graph/badge.svg?token=U475ES0JIL"/>
  </a>

## Why use *Quick SQL test data*?
Writing datasets with SQL  may be tedious and time-consuming because of database integrity constraints.

*This Java library aims to ease the generation of datasets to test SQL queries. It produces INSERT statements taking account of integrity constraints.*

The library automatically:
* identifies *NOT NULL columns* and provides values by requesting the database
* adds rows of dependent tables in case of *foreign key constraints*
* sorts insert statements to accommodate  *foreign key constraints*
* sorts insert statements following *primary key values*

_[Another project](https://github.com/quick-perf/quick-sql-test-data-web) provides a web page to ease the use of the _Quick SQL test data_ library._

## How to use the library

With Maven, you have to add the following dependency:

```xml

<dependency>
    <groupId>org.quickperf</groupId>
    <artifactId>quick-sql-test-data</artifactId>
    <version>0.1</version>
</dependency>
```

You can generate the insert statements with the help of an instance of `org.qstd.QuickSqlTestData` class.

_Quick SQL test data_ works with:
* PostgreSQL
* Oracle
* MariaDB
* MySQL
* Microsoft SQL Server
* H2
* HSQLDB

## Use cases

This library can be helpful in the two following situations.

### Create a dataset before starting the writing of an SQL query

This case happens when you develop SQL queries with *Test-Driven Development* (TDD).

You can read below an example where we define a dataset row for which we generate the INSERT statement:
```java
QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(dataSource);
DatasetRow datasetRow = DatasetRow.ofTable("Player")
                                  .addColumnValue("lastName","Pogba");
List<String> insertStatements = quickSqlTestData.generateInsertListFor(datasetRow);

System.out.println(insertStatements);
```

The console displays the following result:
```
[INSERT INTO PLAYER(FIRSTNAME, LASTNAME) VALUES('Paul', 'Pogba')]
```
FIRSTNAME column owns a NOT NULL constraint. For this reason, the library has retrieved a FIRSTNAME value for the Pogba LASTNAME and has used it in the generated statement.

### Test an existing SQL query
Let's take an example:

```java
QuickSqlTestData quickSqlTestData = QuickSqlTestData.buildFrom(dataSource);
String selectStatement = "SELECT * FROM Player WHERE LASTNAME = 'Pogba'";
String insertScript = quickSqlTestData.generateInsertScriptFor(selectStatement);
System.out.println(insertScript);
```

The console displays the following queries:
```
INSERT INTO TEAM(ID, NAME) VALUES(1, 'Manchester United');
INSERT INTO PLAYER(ID, FIRSTNAME, LASTNAME, TEAM_ID) VALUES(1, 'Paul', 'Pogba', 1);
```
The library has done its best to generate INSERT queries allowing to test the SELECT query.
It has detected a foreign key constraint and has generated a first statement inserting on a Team table. This one contains a value for the NAME column that must not be null.

## License

[Apache License 2.0](/LICENSE.txt)
