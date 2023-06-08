select n.nspname as schema_name,
       c.relname as table_name,
       tn.table_name as table_full_name,
       pk.pk_columns,
       cf.columns,
       -- cf.ordered_columns,
       p.grant_privileges,
       d.comment,
       sp.storage_parameters,
       chk.create_constraints as create_check_constraints,
       fk.drop_constraints,
       uni.create_constraints || fk.create_constraints as create_constraints,
       i.create_indexes,
       i.rename_indexes,
       f.create_functions,
       f.drop_functions,
       f.function_acl_to_grants_params,
       v.drop_views,
       v.create_views,
       v.view_acl_to_grants_params,
       v.comment_views,
       att.alter_sequences,
       tg.create_triggers,
       rl.create_rules,
       case c.relreplident
         when 'd'
           then 'default'
         when 'n'
           then 'nothing'
         when 'f'
           then 'full'
         when 'i'
           then 'index'
       end as replica_identity,
       ind.indexrelid::regclass as replica_identity_index,
       fk.validate_constraints,
       pub.add_publication_names,
       inh.inhparent::pg_catalog.regclass as inhparent,
       pg_catalog.pg_get_expr(c.relpartbound, c.oid) as declarative_partition_expr,
       pg_catalog.pg_get_partition_constraintdef(c.oid) as rebuild_table__partition_constraintdef,
       (select exists (select 1
                         from pg_catalog.pg_inherits chl
                        where chl.inhparent = c.oid)) as is_child_exists
  from pg_class c
 cross join lateral (select c.oid::regclass::text as table_name) tn
 inner join pg_catalog.pg_namespace n
         on n.oid = c.relnamespace
  left join pg_catalog.pg_inherits inh
         on inh.inhrelid = c.oid
  left join pg_index ind
         on ind.indrelid = c.oid and
            ind.indisreplident
 cross join lateral (select array_agg(a.attname order by a.attnum) as columns,
                            array_agg(a.attname order by t.typlen desc, ck.key, t.typname) as ordered_columns
                       from pg_attribute a
                      inner join pg_type t
                              on t.oid = a.atttypid
                       left join pg_constraint pk
                           cross join unnest(pk.conkey) with ordinality ck(key, rn)
                              on pk.conrelid = c.oid and
                                 pk.contype = 'p' and
                                 a.attnum = ck.key
                      where a.attrelid = c.oid and
                            a.attnum >= 0 and
                            not a.attisdropped) cf
  left join lateral (select format('comment on table "%s"."%s__new" is %L;',
                                   n.nspname,
                                   c.relname,
                                   d.description) as comment
                       from pg_description d
                      where d.objoid = c.oid and
                            d.objsubid = 0 and
                            d.classoid = 'pg_class'::regclass) d
         on true
 cross join lateral (select coalesce(array_agg(format('grant %s on table "%s"."%s__new" to "%s";',
                                                      p.privileges,
                                                      n.nspname,
                                                      c.relname,
                                                      p.grantee)),
                                     '{}') as grant_privileges
                       from (select g.grantee, string_agg(g.privilege_type, ', ') as privileges
                               from information_schema.role_table_grants g
                              where g.table_name = c.relname and
                                    g.table_schema = c.relnamespace::regnamespace::text and
                                    g.grantee <> 'postgres'
                              group by g.grantee) p) p
 cross join lateral (select coalesce(json_agg(rd.def), '{}') as create_rules
                       from pg_rewrite rw
                      cross join pg_get_ruledef(rw.oid) as rd(def)
                      where rw.ev_class = c.oid) as rl
 cross join lateral (select coalesce(array_agg(pat.attname order by ck.rn), '{}') as pk_columns
                       from pg_constraint p
                      cross join unnest(p.conkey) with ordinality ck(key, rn)
                      inner join pg_attribute pat
                              on pat.attnum = ck.key and
                                 pat.attrelid = c.oid
                      inner join pg_type ptpt
                              on ptpt.oid = pat.atttypid
                      where p.conrelid = c.oid and
                            p.contype = 'p') pk
 cross join lateral (select coalesce(array_agg(format(
                                                 'alter table %s add constraint %s %s using index %s %s %s;',
                                                 tn.table_name,
                                                 uni.conname,
                                                 case
                                                   when uni.contype = 'p'
                                                     then 'primary key'
                                                   else 'unique'
                                                 end,
                                                 uni.conname,
                                                 case
                                                   when uni.condeferrable
                                                     then 'deferrable'
                                                 end,
                                                 case
                                                   when uni.condeferred
                                                     then 'initially deferred'
                                                 end)),
                                     '{}') as create_constraints
                       from pg_constraint uni
                      where uni.conrelid = c.oid and
                            uni.contype in ('p', 'u')) uni
 cross join lateral (select coalesce(array_agg(format('alter table "%s"."%s__new" add constraint %s %s;',
                                                      n.nspname,
                                                      c.relname,
                                                      chk.conname,
                                                      pg_get_constraintdef(chk.oid))),
                                     '{}') as create_constraints
                       from pg_constraint chk
                      where chk.conrelid = c.oid and
                            chk.contype = 'c') chk
 cross join lateral (select coalesce(array_agg(format('alter table %s add constraint %s %s not valid;',
                                                      fk.conrelid::regclass::text,
                                                      fk.conname,
                                                      pg_get_constraintdef(fk.oid))),
                                     '{}') as create_constraints,
                            coalesce(array_agg(format('alter table %s validate constraint %s;',
                                                      fk.conrelid::regclass::text,
                                                      fk.conname)),
                                     '{}') as validate_constraints,
                            coalesce(array_agg(format('alter table %s drop constraint %s;',
                                                      fk.conrelid::regclass::text,
                                                      fk.conname)),
                                     '{}') as drop_constraints
                       from pg_constraint fk
                      where (fk.conrelid = c.oid
                             or
                             fk.confrelid = c.oid) and
                            fk.contype = 'f') fk
 cross join lateral (select coalesce(array_agg(regexp_replace(replace(regexp_replace(replace(pg_get_indexdef(i.indexrelid),
                                                                                             ic.relname || '" ON ',
                                                                                             substr(ic.relname, 1, 58) || '__new" ON '),
                                                                                     ic.relname || ' ON ',
                                                                                     substr(ic.relname, 1, 58) || '__new ON '),
                                                                      substr(tic.relname, 1, 58) || '" USING ',
                                                                      substr(tic.relname, 1, 58) || '__new" USING '),
                                                             substr(tic.relname, 1, 58) || ' USING ',
                                                             substr(tic.relname, 1, 58) || '__new USING ')
                                               order by cardinality(i.indkey) desc),
                                     '{}') as create_indexes,
                            coalesce(array_agg(format('alter index "%s"."%s" rename to %s;',
                                                      icn.nspname,
                                                      (substr(ic.relname, 1, 58) || '__new')::name,
                                                      ic.relname)),
                                     '{}') as rename_indexes
                       from pg_index i
                      inner join pg_class ic
                              on ic.oid = i.indexrelid
                      inner join pg_class tic
                              on tic.oid = i.indrelid
                      inner join pg_namespace icn
                              on icn.oid = ic.relnamespace
                      where i.indrelid = c.oid) i
 cross join lateral (select coalesce(array_agg(format('create view %s as %s; %s; %s;',
                                                      v.oid::regclass::text,
                                                      replace(replace(replace(pg_get_viewdef(v.oid),
                                                                              format('timezone(''Europe/Moscow''::text, %s.start_time) AS start_time', c.relname),
                                                                              format('%s.start_time', c.relname)),
                                                                      format('timezone(''Europe/Moscow''::text, %s.date_time) AS date_time', c.relname),
                                                                      format('%s.date_time', c.relname)),
                                                              format('timezone(''Europe/Moscow''::text, %s.hit_time) AS hit_time', c.relname),
                                                              format('%s.hit_time', c.relname)),
                                                      (select string_agg(format('%s; %s;', pg_get_functiondef(pgt.tgfoid), pg_get_triggerdef(pgt.oid)), E';\n')
                                                         from pg_trigger pgt
                                                        where pgt.tgrelid = v.oid::regclass),
                                                      (select string_agg(rd.def, E'\n')
                                                         from pg_rewrite rw
                                                        cross join pg_get_ruledef(rw.oid) as rd(def)
                                                        where rw.ev_class = v.oid and
                                                              rw.ev_type <> '1'))
                                               order by v.oid),
                                     '{}') as create_views,
                            coalesce(array_agg(json_build_object('obj_name', v.oid::regclass,
                                                                 'obj_type', 'table',
                                                                 'acl', v.relacl))
                                               filter (where v.relacl is not null),
                                     '{}') as view_acl_to_grants_params,
                            coalesce(array_agg(format('comment on view %s is %L;',
                                                      v.oid::regclass, d.description))
                                              filter (where d.description is not null),
                                     '{}') as comment_views,
                            coalesce(array_agg(format('drop view %s;',
                                                      v.oid::regclass)
                                               order by v.oid desc),
                                     '{}') as drop_views,
                            array_agg(v.reltype) as view_type_oids
                       from pg_class v
                       left join pg_description d
                              on d.objoid = v.oid
                      where v.relkind = 'v' and
                            v.oid in (with recursive w_depend as (
                                        select rw.ev_class
                                          from pg_depend d
                                         inner join pg_rewrite rw
                                                 on rw.oid = d.objid
                                         where d.refobjid = c.oid
                                        union
                                        select rw.ev_class
                                          from w_depend w
                                         inner join pg_depend d
                                                 on d.refobjid = w.ev_class
                                         inner join pg_rewrite rw
                                                 on rw.oid = d.objid
                                      )
                                      select d.ev_class
                                        from w_depend d)) v
 cross join lateral (select coalesce(array_agg(pg_catalog.pg_get_functiondef(f.oid)||';'), '{}') as create_functions,
                            coalesce(array_agg(json_build_object(
                                                'obj_name', format('%s(%s)', f.oid::regproc, pg_get_function_identity_arguments(f.oid)),
                                                'obj_type', case
                                                              when f.prokind = 'p'
                                                                then 'procedure'
                                                              else 'function'
                                                            end,
                                                'acl', f.proacl))
                                             filter (where f.proacl is not null),
                                     '{}') as function_acl_to_grants_params,
                            coalesce(array_agg(format('drop function %s(%s);',
                                                      f.oid::regproc::text,
                                                      pg_get_function_identity_arguments(f.oid))),
                                     '{}') as drop_functions
                       from pg_proc f
                      where f.prorettype = c.reltype
                            or
                            c.reltype = any(f.proargtypes)
                            or
                            c.reltype = any(f.proallargtypes)
                            or
                            f.prorettype = any(v.view_type_oids)) f
 cross join lateral (select coalesce(array_agg(format('alter sequence %s owned by "%s"."%s__new".%s;',
                                                      s.serial_sequence,
                                                      n.nspname,
                                                      c.relname,
                                                      a.attname))
                                              filter (where s.serial_sequence is not null),
                                     '{}') as alter_sequences
                       from pg_attribute a
                      cross join pg_get_serial_sequence(tn.table_name, a.attname) as s(serial_sequence)
                      where a.attrelid = c.oid and
                            a.attnum > 0 and
                            not a.attisdropped) att
 cross join lateral (select coalesce(array_agg(pg_get_triggerdef(tg.oid) || ';'), '{}') as create_triggers
                       from pg_trigger tg
                      where tg.tgrelid = c.oid and
                            not tgisinternal) tg
 cross join lateral (select coalesce(array_agg(format('alter table "%s"."%s__new" set (%s);', n.nspname, c.relname, ro.option)), '{}') as storage_parameters
                       from unnest(c.reloptions) as ro(option)) sp
 cross join lateral (select coalesce(array_agg(format('alter publication %s add table only %s;', pub.pubname, c.oid::regclass)), '{}') as add_publication_names
                       from pg_publication_tables pub
                      where pub.schemaname = c.relnamespace::regnamespace::text and
                            pub.tablename = c.relname) pub
 where n.nspname = $1 and
       c.relname = $2