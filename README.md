# JDBCCopier
A JDBC based database copier

Now with postgres support!

Config example:
```
#
# Use this example configuration for creation of your own config.properties
#

# Source database:
source.type=me.alabor.jdbccopier.database.MSSQLDatabase
source.connectionString=jdbc:sqlserver://localhost;databaseName=mydb;user=sa;password=xxxxx

# Target database:
target.type=me.alabor.jdbccopier.database.PostgreSQLDatabase
target.connectionString=jdbc:postgresql://localhost:5432/mydbonpsql?user=postgres&password=xxxx

# Max. of concurrent threads when copying the data:
maxworkers=5

# Tell which tables to include; dont pass anything if you want to copy all tables
#include=table1,table2

# Tell which tables to exclude; dont pass anything if you want to copy all tables
#exclude=
```
