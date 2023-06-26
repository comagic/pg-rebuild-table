import json
import logging

import asyncpg


class Database:
    conn: asyncpg.Connection = None
    logger = logging.getLogger('Database')

    def __init__(self, host, port, username, password, dbname, lock_timeout, statement_timeout, work_mem, logging_level):
        if logging_level.upper() == 'DEBUG':
            self.logger.setLevel(logging.DEBUG)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.dbname = dbname
        self.server_settings = {
            'application_name': 'pg_rebuild_table',
            'search_path': 'public',
            'lock_timeout': lock_timeout,
            'statement_timeout': statement_timeout,
            'work_mem': work_mem,
        }

    async def start(self):
        self.conn = await asyncpg.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.dbname,
            server_settings=self.server_settings
        )
        await self.conn.set_type_codec(
            'json',
            encoder=lambda x: json.dumps(x, default=str),
            decoder=json.loads,
            schema='pg_catalog'
        )
        self.logger.info(f'Database "{self.dbname}" connection open')

    async def stop(self):
        if not self.conn.is_closed():
            await self.conn.close()
            self.logger.info(f'Database "{self.dbname}" connection closed')
