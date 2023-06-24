import argparse
import asyncio
import logging
import re
import json
from pathlib import Path

import asyncpg
from munch import Munch

from pg_rebuild_table.acl import acl_to_grants
from pg_rebuild_table.connection import Database

__version__ = '0.1.3'


logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)


class PgRebuildTable:
    TABLE_INFO_QUERY = open(Path(__file__).parent / 'sql' / 'table_info_query.sql').read()
    logger = logging.getLogger('PgRebuildTable')
    service_schema = 'rebuild_table'
    min_delta_rows = 10000
    work_mem = '1GB'

    def __init__(
        self,
        db,
        table_full_name,
        additional_condition,
        make_backup,
        clean,
        only_switch,
        only_validate_constraints,
        chunk_limit,
        statement_timeout,
        lock_timeout,
        reorder_columns,
        set_column_order,
        set_data_type,
        logging_level,
    ):
        if logging_level.upper() == 'DEBUG':
            self.logger.setLevel(logging.DEBUG)
        self.db = db
        self.clean = clean
        self.only_steps = []
        if only_switch:
            self.only_steps.append('switch')
        if only_validate_constraints:
            self.only_steps.append('validate_constraints')
        if table_full_name:
            table_full_name = table_full_name.split('.')
            if len(table_full_name) > 1:
                self.schema_name = table_full_name[0]
                self.table_name = table_full_name[1]
            else:
                self.schema_name = 'public'
                self.table_name = table_full_name[0]
        self.additional_condition = additional_condition
        self.make_backup = make_backup
        self.chunk_limit = chunk_limit
        self.statement_timeout = statement_timeout
        self.lock_timeout = lock_timeout
        self.reorder_columns = reorder_columns
        self.set_column_order = set_column_order
        self.set_data_type = set_data_type

    async def _get_table(self):
        self.logger.info(f'Get table info "{self.schema_name}"."{self.table_name}"')
        self.table = Munch.fromDict(
            dict(
                await self.db.conn.fetchrow(
                    self.TABLE_INFO_QUERY,
                    self.schema_name,
                    self.table_name
                )
            )
        )
        self.new_table_full_name = f'"{self.table.schema_name}"."{self.table.table_name}__new"'
        self.delta_table_full_name = f'"{self.table.schema_name}"."{self.table.table_name}__delta"'

    async def _db_exec(self, query):
        if query:
            self.logger.debug(f'db execute {query=}')
            await self.db.conn.execute(query)
            self.logger.debug('db executed')

    async def _cleanup(self, clean=True):
        self.logger.info('structure cleaning')
        async with self.db.conn.transaction():
            await self._db_exec(f"set local lock_timeout = '{self.lock_timeout}';")
            if clean:
                await self._db_exec(f'drop trigger if exists z_rebuild_table__delta on {self.table.table_full_name}')
                await self._db_exec(f'drop table if exists {self.new_table_full_name}')
            else:
                await self._db_exec(f'drop trigger if exists z_rebuild_table__delta on {self.service_schema}."{self.table.schema_name}__{self.table.table_name}"')
            await self._db_exec(f'drop function if exists "{self.table.schema_name}"."{self.table.table_name}__apply_delta"')
            await self._db_exec(f'drop function if exists "{self.table.schema_name}"."{self.table.table_name}__delta"')
            await self._db_exec(f'drop table if exists "{self.table.schema_name}"."{self.table.table_name}__delta"')

    async def _create_table_new(self):
        self.logger.info(f'create table new {self.new_table_full_name}')

        def untype_default(default, column_type):
            return default.replace("'::"+column_type, "'") \
                          .replace("'::public."+column_type[-1], "'") \
                          .replace("'::"+column_type.split('.')[-1], "'")

        columns = []
        for c in self.table.columns:
            column = f'{c.name} {c.type}'
            if c.collate:
                column += f' collate {c.collate}'
            if c.not_null:
                column += f' not null'
            if c.default:
                column += f' default {untype_default(c.default, c.type)}'
            columns.append(column)

        async with self.db.conn.transaction():
            await self._db_exec(f'''create table {self.new_table_full_name}({', '.join(columns)})''')
            await self._db_exec(
                '\n'.join(
                    f'''comment on column {self.new_table_full_name}.{c.name} is {c.comment};'''
                    for c in self.table.columns
                    if c.comment
                )
            )
            await self._db_exec(
                '\n'.join(
                    f'''alter table only {self.new_table_full_name} alter {c.name} set statistics {c.statistics};'''
                    for c in self.table.columns
                    if c.statistics
                )
            )
            await self._db_exec('\n'.join(self.table.storage_parameters))
            await self._db_exec(f'''alter table {self.new_table_full_name} set (autovacuum_enabled = false);''')
            await self._db_exec('\n'.join(self.table.grant_privileges))
            await self._db_exec(f'''alter table {self.new_table_full_name} replica identity {self.table.replica_identity};''')
            await self._db_exec(self.table.comment)
            await self._db_exec('\n'.join(self.table.create_check_constraints))
        self.logger.info('table new created')

    async def _create_trigger_delta_on_table(self):
        self.logger.info('create trigger z_rebuild_table__delta')
        while True:
            try:
                async with self.db.conn.transaction():
                    await self._db_exec(f"set local lock_timeout = '{self.lock_timeout}';")
                    await self._cancel_autovacuum()
                    await self._db_exec(
                        f'''
                        create trigger "z_rebuild_table__delta"
                        after insert or delete or update on "{self.table.schema_name}"."{self.table.table_name}"
                        for each row execute procedure "{self.table.schema_name}"."{self.table.table_name}__delta"();
                        '''
                    )
                break
            except asyncpg.exceptions.LockNotAvailableError:
                self.logger.warning('create trigger failed')
                await asyncio.sleep(20)
                self.logger.info('try create trigger z_rebuild_table__delta')
        self.logger.info('trigger z_rebuild_table__delta created')

    async def _create_objects_delta(self):
        self.logger.info(f'create table delta {self.delta_table_full_name}')
        async with self.db.conn.transaction():
            await self._db_exec(
                f'create unlogged table {self.delta_table_full_name}('
                f'like {self.table.table_full_name} excluding all)'
            )
            await self._db_exec(
                f'''alter table {self.delta_table_full_name} set (autovacuum_enabled = false);'''
            )
            await self._db_exec(
                f'alter table {self.delta_table_full_name} add column delta_id serial;'
                f'alter table {self.delta_table_full_name} add column delta_op "char";'
            )
            await self._db_exec(
                f'''create or replace
                function {self.delta_table_full_name}() returns trigger as $$
                begin
                  if tg_op = 'INSERT' then
                    insert into {self.delta_table_full_name}
                      values (new.*, default, 'i');

                  elsif tg_op = 'UPDATE' then
                    insert into {self.delta_table_full_name}
                      values (new.*, default, 'u');

                  elsif tg_op = 'DELETE' then
                    insert into {self.delta_table_full_name}
                      values (old.*, default, 'd');

                    return old;
                  end if;

                  return new;
                end;
                $$ language plpgsql security definer;'''
            )

            pk_columns = self.table.pk_columns
            columns = ', '.join(f'{c.name}' for c in self.table.columns)
            val_columns = ', '.join(f'r.{c.name}' for c in self.table.columns)
            where = ' and '.join(f't.{c} = r.{c}' for c in pk_columns)
            set_columns = ','.join(f'{c.name} = r.{c.name}'
                                   for c in self.table.columns
                                   if c.name not in pk_columns)

            await self._db_exec(
                f'''create or replace
                    function "{self.table.schema_name}"."{self.table.table_name}__apply_delta"() returns integer as $$
                    declare
                      r record;
                      rows integer := 0;
                    begin
                      for r in with d as (
                                 delete from {self.delta_table_full_name}
                                 returning *
                               )
                               select *
                                 from d
                                order by delta_id
                      loop
                        if r.delta_op = 'i' then
                          insert into {self.new_table_full_name}({columns})
                            values ({val_columns})
                            on conflict do nothing; ''' + (f'''

                        elsif r.delta_op = 'u' then
                          update {self.new_table_full_name} t
                             set {set_columns}
                           where {where}; ''' if set_columns else '') + f'''

                        elsif r.delta_op = 'd' then
                          delete from {self.new_table_full_name} t
                           where {where};
                        end if;

                        rows := rows + 1;
                      end loop;

                      return rows;
                    end;
                    $$ language plpgsql security definer;'''
            )
        self.logger.info(f'table delta {self.delta_table_full_name} created')

    def _get_copy_query(self, pk_value=None):
        self.logger.debug('get incremental query')
        pk_predicate_str = ''
        additional_condition = ''

        if self.additional_condition:
            additional_condition += f'where {self.additional_condition}'

        if pk_value:
            prv_columns = []
            predicate_groups = []
            for k in self.table.pk_columns:
                predicate = ' and '.join(
                    f't.{c} = {pk_value[c]}'
                    for c in prv_columns
                )
                if predicate:
                    predicate += ' and '
                predicate += f'''t.{k} > '{pk_value[k]}' '''
                predicate_groups.append(f'({predicate})')
                prv_columns.append(k)
            pk_predicate_str = f"where ({' or '.join(predicate_groups)})"
        pk_columns = ', '.join(f't.{c}' for c in self.table.pk_columns)
        ins_columns = ', '.join(f'{c.name}' for c in self.table.columns)
        columns = ', '.join(f't.{c.name}' for c in self.table.columns)

        if self.chunk_limit and self.table.pk_columns:
            query = f'''
                with w_t as (
                  select t.*,
                         max(t.___rn) over() ___max_rn
                    from (select t.*,
                                 row_number() over() as ___rn
                            from (select t.*
                                    from {self.table.table_full_name} t
                                   {pk_predicate_str}
                                   order by {pk_columns}
                                   limit {self.chunk_limit}) t) t
                ),
                w_i as (
                  insert into {self.new_table_full_name}({ins_columns})
                    select {columns}
                      from w_t t
                     {additional_condition}
                     order by {pk_columns}
                  returning *
                )
                select (select count(1)
                          from w_i i) as inserted_count,
                       t.*
                  from w_t t
                 where t.___max_rn = t.___rn;
            '''
        else:
            query = f'''
                insert into {self.new_table_full_name}({ins_columns})
                  select {columns}
                    from {self.table.table_full_name} t
                   {additional_condition}
            '''
        self.logger.debug(f'get incremental query \n query={query}')
        return query

    async def _copy_data(self):
        self.logger.info('copy table data')
        if self.chunk_limit:
            pk_value = None
            while True:
                query = self._get_copy_query(pk_value)
                async with self.db.conn.transaction():
                    await self._db_exec(f"set local statement_timeout = {self.statement_timeout};")
                    await self._db_exec(f"set local work_mem = '{self.work_mem}';")
                    pk_value = await self.db.conn.fetchrow(query)
                    if not pk_value:
                        break
        else:
            async with self.db.conn.transaction():
                await self._db_exec(f"set local statement_timeout = {self.statement_timeout};")
                await self._db_exec(f"set local work_mem = '{self.work_mem}';")
                await self._db_exec(self._get_copy_query())
        self.logger.info('table data copied')

    def _get_next_index(self):
        try:
            return self.table.create_indexes.pop()
        except IndexError:
            return None

    async def _cancel_autovacuum(self):
        res = await self._db_exec(
            f'''
            select pg_cancel_backend(pid)
              from pg_stat_activity
             where state = 'active' and
                   backend_type = 'autovacuum worker' and
                   query ~ '{self.table.table_name}';
            '''
        )
        if res:
            self.logger.info('autovacuum canceled')

    async def _analyze(self):
        self.logger.info(f'analyze table {self.new_table_full_name}')
        await self._db_exec(f'analyze {self.new_table_full_name}')
        self.logger.info(f'table {self.new_table_full_name} analyzed')

    async def _create_indexes(self):
        self.logger.info('create indexes')

        if not self.table.create_indexes:
            return

        try:
            while True:
                index_def = self._get_next_index()
                if not index_def:
                    break
                self.logger.info(f'create index {index_def}')
                await self._db_exec(index_def)
                self.logger.info('index created')
        except Exception as e:
            raise e
        self.logger.info('indexes created')

    async def _apply_delta(self):
        self.logger.info('apply data delta')
        rows = await self.db.conn.fetchrow(
            f'''select "{self.table.schema_name}"."{self.table.table_name}__apply_delta"() as rows;'''
        )
        self.logger.info('data delta applied')
        return rows['rows']

    async def _switch_table(self):
        self.logger.info('switch table start')

        while True:
            rows = await self._apply_delta()
            if rows <= self.min_delta_rows:
                break

        if self.table.declarative_partition_expr:
            await self._db_exec(
                f'''
                alter table {self.new_table_full_name} add constraint rebuild_table__partition_constraintdef
                  check {self.table.rebuild_table__partition_constraintdef};'''
            )

        while True:
            try:
                async with self.db.conn.transaction():
                    await self._db_exec(f"set local lock_timeout = '{self.lock_timeout}';")
                    await self._apply_delta()
                    await self._cancel_autovacuum()
                    self.logger.info(f'lock table {self.table.table_full_name}')
                    await self._db_exec(f'lock table {self.table.table_full_name} in access exclusive mode')
                    await self._apply_delta()
                    await self._db_exec('\n'.join(self.table.drop_functions))
                    await self._db_exec('\n'.join(self.table.drop_views))
                    await self._db_exec('\n'.join(self.table.drop_constraints))
                    await self._db_exec('\n'.join(self.table.alter_sequences))

                    if self.table.inhparent:
                        if self.table.declarative_partition_expr:
                            await self._db_exec(f'alter table {self.table.inhparent} detach partition {self.table.table_full_name}')
                        else:
                            await self._db_exec(f'alter table {self.table.table_full_name} no inherit {self.table.inhparent}')

                    if self.make_backup:
                        self.logger.info(f'backup table {self.table.table_name}')
                        await self._db_exec(f'alter table {self.table.table_full_name} rename to "{self.table.schema_name}__{self.table.table_name}";')
                        await self._db_exec(f'alter table "{self.table.schema_name}"."{self.table.schema_name}__{self.table.table_name}" set schema {self.service_schema};')
                    else:
                        self.logger.info(f'drop table {self.table.table_name}')
                        await self._db_exec(f'drop table {self.table.table_full_name};')

                    await self._cleanup(False)
                    self.logger.info(f'rename table {self.table.table_name}__new -> {self.table.table_name}')
                    await self._db_exec(f'alter table {self.new_table_full_name} rename to "{self.table.table_name}";')

                    if self.table.inhparent:
                        self.logger.info(f'attach partition {self.table.table_name}')
                        if self.table.declarative_partition_expr:
                            await self._db_exec(f'alter table {self.table.inhparent} attach partition {self.table.table_full_name} {self.table.declarative_partition_expr}')
                            await self._db_exec(f'alter table {self.table.table_full_name} drop constraint rebuild_table__partition_constraintdef;')
                        else:
                            await self._db_exec(f'alter table {self.table.table_full_name} inherit {self.table.inhparent}')

                    await self._db_exec('\n'.join(self.table.rename_indexes))
                    await self._db_exec('\n'.join(self.table.create_constraints))
                    await self._db_exec('\n'.join(self.table.create_rules))
                    await self._db_exec('\n'.join(self.table.create_triggers))
                    await self._db_exec('\n'.join(self.table.create_views))
                    await self._db_exec('\n'.join(self.table.comment_views))
                    await self._db_exec(
                        '\n'.join((
                            acl_to_grants(
                                params['acl'],
                                'column',
                                self.table.table_full_name,
                                params['name'])
                            for params in self.table.columns
                            if params['acl']
                        ))
                    )
                    await self._db_exec(
                        '\n'.join((
                            acl_to_grants(
                                params['acl'],
                                params['obj_type'],
                                params['obj_name'])
                            for params in self.table.view_acl_to_grants_params
                        ))
                    )
                    await self._db_exec('\n'.join(self.table.create_functions))
                    await self._db_exec(
                        '\n'.join((
                            acl_to_grants(
                                params['acl'],
                                params['obj_type'],
                                params['obj_name'])
                            for params in self.table.function_acl_to_grants_params
                        ))
                    )
                    await self._db_exec('\n'.join(self.table.add_publication_names))
                    await self._db_exec(f'alter table {self.table.table_full_name} reset (autovacuum_enabled);')
                    break
            except asyncpg.exceptions.LockNotAvailableError:
                self.logger.warning('lock table failed')
                await asyncio.sleep(20)
                self.logger.info('try lock table')
                await self._apply_delta()
            except Exception as e:
                self.logger.error(f'switch table: {e}')
                raise

        self.logger.info('switch table done')

    async def _validate_constraints(self):
        self.logger.info('validate constraints')
        if not self.table.validate_constraints:
            return
        for c in self.table.validate_constraints:
            self.logger.info(re.sub('alter table (.*) validate constraint (.*);', '\\1: \\2', c))
            try:
                await self._db_exec(c)
            except Exception:
                self.logger.warning(re.sub('alter table (.*) validate constraint (.*) failed;', '\\1: \\2', c))
        self.logger.info('constraints validated')

    async def start(self):
        await self._get_table()

        if not self.table:
            self.logger.warning('Metadata for table is not defined')
            return

        if self.table.is_child_exists:
            logging.warning("Can't rebuild parent partition")
            return

        if not self.table.pk_columns:
            self.logger.error('The table does not have a primary key...')
            return

        if self.clean:
            await self._cleanup()
            return

        if self.reorder_columns:
            self.table.columns = self.table.ordered_columns

        if self.set_column_order:
            new_columns = []
            for column_name in self.set_column_order:
                new_columns.extend(c for c in self.table.columns if c.name == column_name)
            if len(new_columns) != len(self.table.columns):
                self.logger.error('Parameter "set_column_order" with list of columns specified incorrectly...')
                return
            self.table.columns = new_columns

        if self.set_data_type:
            for ct in self.set_data_type:
                for i, c in enumerate(self.table.columns):
                    if c.name == ct['name'] and c.type != ct['type']:
                        self.table.columns[i]['type'] = ct['type']

        # FIXME: схема должна создаваться при создании extension
        await self._db_exec(f'create schema if not exists "{self.service_schema}";')
        await self._db_exec(
            f'''
            create table if not exists "{self.service_schema}"."table"(
              schema_name text,
              table_name text,
              last_start_time timestamp,
              last_stop_time timestamp,
              before_table_size bigint,
              before_total_size bigint,
              after_table_size bigint,
              after_total_size bigint,
              constraint pk_table primary key(schema_name, table_name));'''
        )
        if not self.only_steps:
            await self._db_exec(
                f'''
                insert into "{self.service_schema}"."table"(schema_name, table_name, last_start_time, before_table_size, before_total_size)
                  values ('{self.table.schema_name}',
                          '{self.table.table_name}',
                          now(),
                          pg_table_size('{self.table.table_full_name}'),
                          pg_total_relation_size('{self.table.table_full_name}'))
                on conflict
                on constraint pk_table
                do update set last_start_time = now();'''
            )
            await self._create_table_new()
            await self._create_objects_delta()
            await self._create_trigger_delta_on_table()
            await self._copy_data()
            await self._create_indexes()
            await self._analyze()

        if 'switch' in self.only_steps or not self.only_steps:
            await self._switch_table()
            await self._db_exec(
                f'''
                update "{self.service_schema}"."table" t
                   set after_table_size = pg_table_size('{self.table.table_full_name}'),
                       after_total_size = pg_total_relation_size('{self.table.table_full_name}')
                 where t.schema_name = '{self.table.schema_name}' and
                       t.table_name = '{self.table.table_name}' '''
            )
        if 'validate_constraints' in self.only_steps or not self.only_steps:
            await self._validate_constraints()

        await self._db_exec(
            f'''
            update "{self.service_schema}"."table" t
               set last_stop_time = now()
             where t.schema_name = '{self.table.schema_name}' and
                   t.table_name = '{self.table.table_name}' '''
        )

    async def stop(self):
        if not self.only_steps:
            await self._cleanup()


class Command:
    logger = logging.getLogger('Command')

    def __init__(self):
        arg_parser = argparse.ArgumentParser(
            description='Rebuild table ',
            epilog='Report bugs to <viktor-b_90@inbox.ru>.',
            conflict_handler='resolve'
        )
        arg_parser.add_argument(
            '--version',
            action='version',
            version=__version__
        )
        arg_parser.add_argument(
            '--clean',
            action="store_true",
            help='clean out_dir if not empty.'
        )
        arg_parser.add_argument(
            '-h', '--host',
            type=str,
            help='host for connect db.'
        )
        arg_parser.add_argument(
            '-p', '--port',
            type=str,
            help='port for connect db.'
        )
        arg_parser.add_argument(
            '-U',
            '--username',
            type=str,
            help='user for connect db.'
        )
        arg_parser.add_argument(
            '-W',
            '--password',
            type=str,
            help='password for connect db.'
        )
        arg_parser.add_argument(
            '-j',
            '--jobs',
            type=int,
            help='number of connections.',
            default=2
        )
        arg_parser.add_argument(
            '-T',
            '--table_full_name',
            type=str,
            help='table full name.',
        )
        arg_parser.add_argument(
            '-ac',
            '--additional_condition',
            type=str,
            help='additional condition for copying data.'
        )
        arg_parser.add_argument(
            '-cl',
            '--chunk_limit',
            type=str,
            help='numerical value of the chunk size limit for data transfer. by default the table overlaps completely in one pass.'
        )
        arg_parser.add_argument(
            '-st',
            '--statement_timeout',
            type=str,
            help='maximum request execution time.',
            default=900000
        )
        arg_parser.add_argument(
            '-lt',
            '--lock_timeout',
            type=str,
            help='specifying the period of time that must elapse before an attempt to acquire a lock is abandoned. (example: 1s or 1min or 1h)',
            default='1s'
        )
        arg_parser.add_argument(
            '--make_backup',
            action="store_true",
            help='make a table backup',
        )
        arg_parser.add_argument(
            '--only_switch',
            action="store_true",
            help='only switch if exists table "table_full_name__new"->"table_full_name"',
        )
        arg_parser.add_argument(
            '--only_validate_constraints',
            action="store_true",
            help='only validate constraint on "table_full_name"',
        )
        arg_parser.add_argument(
            '--reorder_columns',
            action="store_true",
            help='Reorders columns to reduce the physical disk space required to store data tuples.',
        )
        arg_parser.add_argument(
            '--set_column_order',
            type=lambda s: [str(item) for item in s.split(',')],
            help='Сhange column order.',
        )
        arg_parser.add_argument(
            '--set_data_type',
            type=json.loads,
            help='Сhange column data type.',
        )
        arg_parser.add_argument(
            '-d',
            '--dbname',
            help='source database name'
        )
        arg_parser.add_argument(
            '-ll',
            '--logging_level',
            default='INFO',
            help='Logging Level'
        )
        args = arg_parser.parse_args()

        db = Database(
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            dbname=args.dbname,
            logging_level=args.logging_level
        )

        pg_rebuild_table = PgRebuildTable(
            db,
            table_full_name=args.table_full_name,
            additional_condition=args.additional_condition,
            make_backup=args.make_backup,
            clean=args.clean,
            only_switch=args.only_switch,
            only_validate_constraints=args.only_validate_constraints,
            chunk_limit=args.chunk_limit,
            statement_timeout=args.statement_timeout,
            lock_timeout=args.lock_timeout,
            reorder_columns=args.reorder_columns,
            set_column_order=args.set_column_order,
            set_data_type=args.set_data_type,
            logging_level=args.logging_level
        )

        self.components = [db, pg_rebuild_table]

    async def start(self):
        try:
            for c in self.components:
                await c.start()
        except Exception:
            raise Exception('Component startup error')

    async def stop(self):
        for c in reversed(self.components):
            try:
                await c.stop()
            except Exception as e:
                self.logger.warning(f'Component termination error: {e}')


def main():
    loop = asyncio.get_event_loop()
    cmd = Command()
    try:
        loop.run_until_complete(cmd.start())
    except Exception:
        raise Exception('pg_rebuild_table error...')
    finally:
        loop.run_until_complete(cmd.stop())


if __name__ == "__main__":
    main()
