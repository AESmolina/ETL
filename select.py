def select_from_db(col, base, table):
    return(
        f"""SELECT 
                {col}
            FROM {base}.{table}
            """)


def insert_to_db(col, base, table):
    col_list = col.split()
    count_col = len(col_list)
    str = '%s, ' * count_col
    return(
         f""" INSERT INTO {base}.{table}(
                 {col})
              VALUES({str[:-2]})""")



def insert_to_meta(xxxx_meta,xxxx_source):
    return(f"""insert into demipt3.{xxxx_meta}( schema_name, table_name, max_update_dt)
               values( 'demipt3','{xxxx_source}', to_timestamp('1900-01-01','YYYY-MM-DD'));""")

def select_mudt_from_meta(xxxx_meta,xxxx_source):
    return (f"""select 
                        max_update_dt 
                      from demipt3.{xxxx_meta} 
                      where schema_name='demipt3' and table_name='{xxxx_source}';""")


# Инкрементальная загрузка
def scd2_incr(base, xxxx_stg_del, xxxx_source, xxxx_target, xxxx_stg, col, col_tgt, xxxx_meta):
    column = col.split(', ')
    tgt_list = col_tgt.split(', ')
    stg_col = ''
    tgt_str_col = ''
    str = ''

    for i in column[:-1]:
        stg_col = stg_col + ('stg.' + i) + ', '

    for i in tgt_list[:-3]:
        tgt_str_col = tgt_str_col + ('tgt.' + i) + ', '

    for tgt_col, stg_column in zip(tgt_list[1:-3], column[1:-2]):
        str = str + f"""stg.{stg_column} <> tgt.{tgt_col} or (stg.{stg_column} is null and tgt.{tgt_col} is not null) or 
        (stg.{stg_column} is not null and tgt.{tgt_col} is null)""" + ' or '
    return (
            f""" 
            -- 4. Загрузка в приемник "вставок" на источнике (формат SCD2).
            
                insert into {base}.{xxxx_target}({col_tgt})
                select
                    {stg_col}
                    to_date('9999-12-31', 'YYYY-MM-DD'),
                    'N'
                from {base}.{xxxx_stg} stg
                left join
                {base}.{xxxx_target} tgt
                on
                stg.{column[0]} = tgt.{tgt_list[0]}
                where
                tgt.{tgt_list[0]} is null;

            -- 5. Обновление в приемнике "обновлений" на источнике (формат SCD2).
            
            update {base}.{xxxx_target}
            set
                effective_to = tmp.update_dt - interval '1 second'
            from
            (
                select
                    stg.{column[0]},
                    stg.update_dt
                from {base}.{xxxx_stg} stg
                inner join {base}.{xxxx_target} tgt
                on stg.{column[0]} = tgt.{tgt_list[0]}
                   and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD')
                   and tgt.deleted_flg = 'N'
            where 1=0 or
                {str[:-4]}) tmp
            where
                {xxxx_target}.{tgt_list[0]} = tmp.{column[0]}
                and {xxxx_target}.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD')
                and {xxxx_target}.deleted_flg = 'N';
                
            insert into {base}.{xxxx_target}({col_tgt})
            select
                    {stg_col}
                    to_date('9999-12-31', 'YYYY-MM-DD'),
                    'N'
            from {base}.{xxxx_stg} stg
            inner join {base}.{xxxx_target} tgt
            on stg.{column[0]} = tgt.{tgt_list[0]}
               and tgt.effective_to = stg.update_dt - interval '1 second'
               and tgt.deleted_flg = 'N'
            where 1=0 or
                {str[:-4]};
             
             -- 6 Удалить удаленные записи.
               
            insert into {base}.{xxxx_target}({col_tgt})
            select 
                {tgt_str_col}
                now(),
                to_date('9999-12-31', 'YYYY-MM-DD'),
                'Y'
            from {base}.{xxxx_target} tgt
            left join {base}.{xxxx_stg_del} stg
            on tgt.{tgt_list[0]} = stg.id
            where
                stg.id is null and tgt.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and tgt.deleted_flg = 'N';

           update {base}.{xxxx_target}
            set
                effective_to = now() - interval '1 second'
            where
                {xxxx_target}.{tgt_list[0]} in (
                    select
                        tgt.{tgt_list[0]}
                    from {base}.{xxxx_target} tgt
                    left join {base}.{xxxx_stg_del} stg
                    on tgt.{tgt_list[0]} = stg.id
                    where
                     stg.id is null 
                     and tgt.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') 
                     and tgt.deleted_flg = 'N'
                )
                and {xxxx_target}.effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
                and {xxxx_target}.deleted_flg = 'N'; 
                
            update demipt3.{xxxx_meta}
                set
            max_update_dt = coalesce((select max(coalesce(update_dt,create_dt + interval '1 second')) from {base}.{xxxx_stg} ), 
            (select max_update_dt from demipt3.{xxxx_meta} where schema_name='demipt3' and table_name='{xxxx_source}') )
                where
            schema_name = 'demipt3' and table_name = '{xxxx_source}';""")


def insert_to_fact_table(base,xxxx_target, xxxx_stg, col, col_tgt):
    column = col.split(', ')
    tgt_list = col_tgt.split(', ')
    stg_col = ''
    for i in column:
        stg_col = stg_col + ('stg.' + i) + ', '
    return (
        f""" 
                insert into {base}.{xxxx_target}({col_tgt})
                select
                    {stg_col[:-2]}
                from {base}.{xxxx_stg} stg
                left join
                {base}.{xxxx_target} tgt
                on
                stg.{column[0]} = tgt.{tgt_list[0]}
                where
                tgt.{tgt_list[0]} is null;""")


def scd2_without_crdt_updt(base, xxxx_stg_del, xxxx_target, xxxx_stg, col, col_tgt, create_dt):
    column = col.split(', ')
    tgt_list = col_tgt.split(', ')
    stg_col = ''
    tgt_str_col = ''
    str = ''

    for i in column:
        stg_col = stg_col + ('stg.' + i) + ', '

    for i in tgt_list[:-3]:
        tgt_str_col = tgt_str_col + ('tgt.' + i) + ', '

    for tgt_col, stg_column in zip(tgt_list[1:-3], column[1:]):
        str = str + f"""stg.{stg_column} <> tgt.{tgt_col} or (stg.{stg_column} is null and tgt.{tgt_col} is not null) or 
        (stg.{stg_column} is not null and tgt.{tgt_col} is null)""" + ' or '
    return (
        f""" 
            -- 4. Загрузка в приемник "вставок" на источнике (формат SCD2).

                insert into {base}.{xxxx_target}({col_tgt})
                select
                    {stg_col}
                    to_date('{create_dt}', 'YYYY-MM-DD'),
                    to_date('9999-12-31', 'YYYY-MM-DD'),
                    'N'
                from {base}.{xxxx_stg} stg
                left join
                {base}.{xxxx_target} tgt
                on
                stg.{column[0]} = tgt.{tgt_list[0]}
                where
                tgt.{tgt_list[0]} is null;

            -- 5. Обновление в приемнике "обновлений" на источнике (формат SCD2).

            update {base}.{xxxx_target}
            set
                effective_to = to_date('{create_dt}', 'YYYY-MM-DD') - interval '1 second'
            from
            (
                select
                    stg.{column[0]}
                from {base}.{xxxx_stg} stg
                inner join {base}.{xxxx_target} tgt
                on stg.{column[0]} = tgt.{tgt_list[0]}
                   and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD')
                   and tgt.deleted_flg = 'N'
            where 1=0 or
                {str[:-4]}) tmp
            where
                {xxxx_target}.{tgt_list[0]} = tmp.{column[0]}
                and  {xxxx_target}.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD')
                and  {xxxx_target}.deleted_flg = 'N';

            insert into {base}.{xxxx_target}({col_tgt})
            select
                    {stg_col}
                    to_date('{create_dt}', 'YYYY-MM-DD'),
                    to_date('9999-12-31', 'YYYY-MM-DD'),
                    'N'
            from {base}.{xxxx_stg} stg
            inner join {base}.{xxxx_target} tgt
            on stg.{column[0]} = tgt.{tgt_list[0]}
               and tgt.effective_to = to_date('{create_dt}', 'YYYY-MM-DD') - interval '1 second'
               and tgt.deleted_flg = 'N'
            where 1=0 or
                {str[:-4]};

             -- 6 Удалить удаленные записи.

            insert into {base}.{xxxx_target}({col_tgt})
            select 
                {tgt_str_col}
                now(),
                to_date('9999-12-31', 'YYYY-MM-DD'),
                'Y'
            from {base}.{xxxx_target} tgt
            left join {base}.{xxxx_stg_del} stg
            on tgt.{tgt_list[0]} = stg.id
            where
                stg.id is null and tgt.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and tgt.deleted_flg = 'N';

           update {base}.{xxxx_target}
            set
                effective_to = now() - interval '1 second'
            where
                {xxxx_target}.{tgt_list[0]} in (
                    select
                        tgt.{tgt_list[0]}
                    from {base}.{xxxx_target} tgt
                    left join {base}.{xxxx_stg_del} stg
                    on tgt.{tgt_list[0]} = stg.id
                    where
                     stg.id is null 
                     and tgt.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') 
                     and tgt.deleted_flg = 'N'
                )
                and {xxxx_target}.effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
                and {xxxx_target}.deleted_flg = 'N'; """)

