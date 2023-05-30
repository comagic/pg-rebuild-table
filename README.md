# pg_rebuild_table -- Rebuild tables in PostgreSQL databases with minimal locks
================================================================================

Installation:
--------------------
- 1 step
  ``git clone git@github.com:comagic/pg-rebuild-table.git``

- 2 step
  ``pip install -e ./pg-rebuild-table/``

Restrictions:
--------------------
- Only superusers can use the utility.
- Target table must have a PRIMARY KEY.

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
            Force pg_rebuild_table to prompt for a password before connecting to a database.

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
            An optional condition on which the data will be recollected

        -cl
        --chunk_limit
            Data packet size when rebuilding tabular data collection. By default the table overlaps completely in one pass'

        -st
        --statement_timeout
            Abort any statement that takes more than the specified number of milliseconds, starting from the time the command arrives at the server from the client. The default is 900000 seconds

        -lt
        --lock_timeout
            Abort any statement that waits longer than the specified number of milliseconds while attempting to acquire a lock on a table, index, row, or other database object. default 1 second.

        --make_backup
            If the parameter is set, then the old version of the table is migrated along with the data to the rebuild_table schema.

        --only_switch
            If the parameter is set, then only the replacement of the old table with the new assembled table is performed. It is relevant if the script prepared a new table with data, but could not perform the substitution for a long time. ("table_full_name__new"->"table_full_name")

        --only_validate_constraints
            If the parameter is set, then invalid constraints on the table are searched for and validation is started.
