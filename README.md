# SQL test data generator

  <a href="https://search.maven.org/search?q=org.quickperf">
    <img src="https://maven-badges.herokuapp.com/maven-central/org.quickperf/sql-test-data-generator/badge.svg"
         alt="Maven Central">
  </a>
  &nbsp;&nbsp;
  <a href="https://github.com/quick-perf/sql-test-data-generator/blob/master/LICENSE.txt">
    <img src="https://img.shields.io/badge/license-Apache2-blue.svg"
         alt = "License">
  </a>
  &nbsp;&nbsp;
  <a href="https://github.com/quick-perf/sql-test-data-generator/actions?query=workflow%3ACI">
    <img src="https://img.shields.io/github/workflow/status/quick-perf/sql-test-data-generator/CI"
         alt = "Build Status">
  </a>
  &nbsp;
  <a href="https://codecov.io/gh/quick-perf/sql-test-data-generator">
    <img src="https://codecov.io/gh/quick-perf/sql-test-data-generator/branch/main/graph/badge.svg?token=U475ES0JIL"/>
  </a>

## Why using *SQL test data generator*?
Writing datasets with SQL  may be tedious and time-consuming because of database integrity constraints.

*This Java library aims to ease the generation of datasets to test SQL queries. It produces INSERT statements taking account of integrity constraints.*

The library automatically:
* identifies *NOT NULL columns* and provides values by requesting the database
* adds rows of dependent tables in case of *foreign key constraints*
* sorts insert statements to accommodate  *foreign key constraints*
* sorts insert statements following *primary key values*

## How to use the library

With Maven, you have to add the following dependency:

```xml

<dependency>
    <groupId>org.quickperf</groupId>
    <artifactId>sql-test-data-generator</artifactId>
    <version>0.1-SNASPHOT</version>
</dependency>
```

You can generate the insert statements with the help of an instance of `org.stdg.SqlTestDataGenerator` class.

_SQL test data generator_ works with:
* H2
* HSQLDB
* MariaDB
* Microsoft SQL Server
* MySQL
* PostgreSQL

## Use cases

This library can be helpful in the two following situations.

### Create a dataset before starting the writing of an SQL query

This case happens when you develop SQL queries with *Test-Driven Development* (TDD).

You can read below an example where we define a dataset row for which we generate the INSERT statement:
```java
SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(dataSource);
DatasetRow datasetRow = DatasetRow.ofTable("Player")
                                  .addColumnValue("lastName","Pogba");
List<String> insertStatements = sqlTestDataGenerator.generateInsertListFor(datasetRow);

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
SqlTestDataGenerator sqlTestDataGenerator = SqlTestDataGenerator.buildFrom(dataSource);
String selectStatement = "SELECT * FROM Player WHERE LASTNAME = 'Pogba'";
String insertScript = sqlTestDataGenerator.generateInsertScriptFor(selectStatement);
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
