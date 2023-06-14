# pg_rebuild_table -- Rebuild tables in PostgreSQL databases with minimal locks
================================================================================

Installation:
--------------------
- **1 step**

  ``git clone https://github.com/comagic/pg-rebuild-table.git``

- **2 step**

  ``pip install -e ./pg-rebuild-table/``

Dependency:
--------------------
- python>=3.8

Restrictions:
--------------------
- Only superusers can use the utility.
- Target table must have a PRIMARY KEY.
- Trigger "z_rebuild_table__delta" must be the last trigger in the "before" set.

Basic approach:
--------------------
1. create new tables TABLE_NAME__new and TABLE_NAME__delta
2. create trigger z_rebuild_table__delta wich fixing all changes from TABLE_NAME to TABLE_NAME__delta
3. copy data from TABLE_NAME to TABLE_NAME__new
4. create indexes for TABLE_NAME__new
5. analyze TABLE_NAME__new
6. apply delta from TABLE_NAME__delta to TABLE_NAME__new (in loop while last rows > 10000)
7. switch TABLE_NAME to TABLE_NAME__new
8. start transaction begin;
8.1. exclusive lock TABLE_NAME;
8.2. apply delta
8.3. drop depend functions, views, constraints;
8.4. link sequences to TABLE_NAME__new
8.5. drop table TABLE_NAME;
8.6. rename table TABLE_NAME__new to TABLE_NAME;
8.7. create depend functions, triggers, views, constraints (not valid), rules, add to publications;
8.8 commit;
9. validate constraints

********************
Tested on PostgreSql 12.*
********************

Options:
--------------------
    Common options:
        --version
            Print the pg_rebuild_table version and exit.

        --help
            Show help about pg_rebuild_table command line arguments, and exit.

        --clean
            Clear auxiliary objects pg_rebuild_table.

    Connection Options:
        -h
        --host
            Specifies the host name of the machine on which the server is running. If the value begins with a slash, it is used as the directory for the Unix domain socket.

        -p
        --port
            Specifies the TCP port on which the server is listening for connections.

        -U
        --username
            User name to connect as.

        -W
        --password
            Password to connect to the database.

        -d
        --dbname
        dbname
            specifies the name of the database in which the table will be rebuilt.

    Rebuild options:
        -T
        --table_full_name
            Full table name to rebuild with pg_rebuild_table

        -ac
        --additional_condition
            An optional parameter in which you can set a parent for pouring data into a table with a new structure. (example: 't.group_id in (select g.id from group g where not g.is_removed)')

        -cl
        --chunk_limit
            An optional parameter that specifies the size of data portions that will be poured into a table with a new structure, which will be split into separate transactions.
By default, table data overflows in one pass.

        -st
        --statement_timeout
            Abort any statement that takes more than the specified number of milliseconds, starting from the time the command arrives at the server from the client. The default is 900000 seconds.

        -lt
        --lock_timeout
            Abort any statement that waits longer than the specified number of milliseconds while attempting to acquire a lock on a table, index, row, or other database object. default 1 second.

        --make_backup
            If the parameter is set, then the old version of the table is not deleted, but migrated along with the data to the rebuild_table schema.

        --only_validate_constraints
            If the parameter is set, then only the search for invalid constraints for the table is performed and validation is started.

        --reorder_columns
            If the parameter is set, then the order of the columns is determined in such a way that the data tuple occupies the minimum disk space.

        --set_column_order
            The parameter is passed a list of columns that determines the new order in which they are placed. (example: 'col1,col2,col3')

        --set_data_type
            The parameter is passed a list of dictionaries in which the new column type is specified. (example: [{"name":"col1", "type":"bigint"}])


Examples:
--------------------
- **Rebuild the table with data that satisfies the condition. transfusion of data to carry out in chunks of 100,000 lines.**

``pg_rebuild_table -p 5432 -h /tmp -d database_name --chunk_limit 100000 -T employee -ac 't.app_id in (select app.id from app)'``

``pg_rebuild_table -p 5432 -h /tmp -d database_name --chunk_limit 100000 -T employee -ac 't.group_id in (43597,43789,43791,44229)'``

- **Rebuild the data table with automatic reordering of columns for better storage of data tuples. transfusion of data should be carried out in portions of 100,000 lines. Sometimes compresses the amount of data.**

``pg_rebuild_table -p 5432 -h /tmp -d database_name --chunk_limit 100000 -T employee --reorder_columns``

- **When rebuilding the table, change the order of the columns.**

``pg_rebuild_table -p 5432 -h /tmp -d database_name -T employee --set_column_order id,app_id,first_visit,url,title,site_id``

- **When rebuilding the table, change the data type of the "app_id" and "group_id" columns from "int" to "bigint".**

``pg_rebuild_table -p 5432 -h /tmp -d database_name -T employee --set_data_type '[{"name":"app_id", "type":"bigint"}, {"name":"group_id", "type":"bigint"}]'``
