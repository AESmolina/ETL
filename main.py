#!/usr/bin/python3

# Данная программа осуществляет ETL процесс, получающий ежедневную выгрузку данных
# (предоставляется за 3 дня), загружающий ее в хранилище данных и ежедневно строящий отчет.

#Признаки мошеннических операций (event_type).
#1. Совершение операции при просроченном или заблокированном паспорте.
#2. Совершение операции при недействующем договоре.
#3. Совершение операций в разных городах в течение одного часа.
#4. Попытка подбора суммы.

import psycopg2
import pandas as pd
import os
import glob
from sql_scripts.select import *
import datetime

# Создание подключения к PostgreSQL - хранилищу
conn_bank = psycopg2.connect(database="bank",
                             host="**********************",
                             user="bank_etl",
                             password="*************",
                             port="5432")

conn_dwh = psycopg2.connect(database="edu",
                            host="**********************",
                            user="demipt3",
                            password="*******************",
                            port="5432")

# Отключение автокоммита
conn_dwh.autocommit = False

# Создание курсора
cursor_bank = conn_bank.cursor()
cursor_dwh = conn_dwh.cursor()

################################################
# Очистка стейджинговых таблиц

cursor_dwh.execute("delete from demipt3.smla_stg_transactions")
cursor_dwh.execute("delete from demipt3.smla_stg_passport_blacklist")
cursor_dwh.execute("delete from demipt3.smla_stg_terminals")
cursor_dwh.execute("delete from demipt3.smla_stg_cards")
cursor_dwh.execute("delete from demipt3.smla_stg_accounts")
cursor_dwh.execute("delete from demipt3.smla_stg_clients")
cursor_dwh.execute("delete from demipt3.smla_stg_del_terminals")
cursor_dwh.execute("delete from demipt3.smla_stg_del_cards")
cursor_dwh.execute("delete from demipt3.smla_stg_del_accounts")
cursor_dwh.execute("delete from demipt3.smla_stg_del_clients")

################################################
#Вставка первых значений в таблицы метаданных
# meta_cards
col = 'schema_name, table_name, max_update_dt'
base = 'demipt3'
# Проверка на пустую таблицу
cursor_dwh.execute(select_from_db(col, base, 'smla_meta_cards'))
records = cursor_dwh.fetchall()
names = [x[0] for x in cursor_dwh.description]
df = pd.DataFrame(records, columns=names)
if df.shape[0] == 0:
    cursor_dwh.execute(insert_to_meta('smla_meta_cards', 'cards'))
    conn_dwh.commit()

# meta_accounts
# Проверка на пустую таблицу
cursor_dwh.execute(select_from_db(col, base, 'smla_meta_accounts'))
records = cursor_dwh.fetchall()
names = [x[0] for x in cursor_dwh.description]
df = pd.DataFrame(records, columns=names)
if df.shape[0] == 0:
    cursor_dwh.execute(insert_to_meta('smla_meta_accounts', 'accounts'))
    conn_dwh.commit()

# meta_clients
# Проверка на пустую таблицу
cursor_dwh.execute(select_from_db(col, base, 'smla_meta_clients'))
records = cursor_dwh.fetchall()
names = [x[0] for x in cursor_dwh.description]
df = pd.DataFrame(records, columns=names)
if df.shape[0] == 0:
    cursor_dwh.execute(insert_to_meta('smla_meta_clients', 'clients'))
    conn_dwh.commit()

################################################
#cards

# Захват данных из источника в стейджинги
xxxx_meta = 'smla_meta_cards'
xxxx_source = 'cards'

# Выбор max_update_dt из meta_cards
cursor_dwh.execute(select_mudt_from_meta(xxxx_meta, xxxx_source))

records = cursor_dwh.fetchall()
names = [x[0] for x in cursor_dwh.description]
df = pd.DataFrame(records, columns=names)

# stg_cards
col = """card_num, account, create_dt, update_dt"""
base_source = 'info'
table_source = 'cards'
base = 'demipt3'
xxxx_stg = 'smla_stg_cards'

cursor_bank.execute(f"""select {col} from {base_source}.{table_source}
                    where coalesce(update_dt, create_dt) >= '{df['max_update_dt'][0]}';""")

records = cursor_bank.fetchall()
names = [x[0] for x in cursor_bank.description]
df = pd.DataFrame(records, columns=names)

# Загрузка данных в стейджинг
cursor_dwh.executemany(insert_to_db(col, base, xxxx_stg), df.values.tolist())

#Захват id из источника в smla_stg_del_cards
id = 'card_num'
cursor_bank.execute(select_from_db(id, base_source, table_source))

records = cursor_bank.fetchall()
names = [x[0] for x in cursor_bank.description]
df = pd.DataFrame(records, columns=names)

# Загрузка id в stg_del
xxxx_stg_del = 'smla_stg_del_cards'
cursor_dwh.executemany(insert_to_db('id', base, xxxx_stg_del), df.values.tolist())

#Инкриментальная загрузка в smla_dwh_dim_cards_hist
xxxx_target = 'smla_dwh_dim_cards_hist'
col_tgt = 'card_num, account_num, effective_from, effective_to, deleted_flg'
cursor_dwh.execute(scd2_incr(base, xxxx_stg_del, xxxx_source, xxxx_target, xxxx_stg, col, col_tgt, xxxx_meta))

###############################################################
#accounts
# Захват данных из источника в стейджинги
xxxx_meta = 'smla_meta_accounts'
xxxx_source = 'accounts'

# Выбор max_update_dt из meta_accounts
cursor_dwh.execute(select_mudt_from_meta(xxxx_meta, xxxx_source))

records = cursor_dwh.fetchall()
names = [x[0] for x in cursor_dwh.description]
df = pd.DataFrame(records, columns=names)

# stg_accounts
col = 'account, valid_to, client, create_dt, update_dt'
base_source = 'info'
table_source = 'accounts'
base = 'demipt3'
xxxx_stg = 'smla_stg_accounts'

cursor_bank.execute(f"""select {col} from {base_source}.{table_source}
                    where coalesce(create_dt,update_dt) >= '{df['max_update_dt'][0]}';""")

records = cursor_bank.fetchall()
names = [x[0] for x in cursor_bank.description]
df = pd.DataFrame(records, columns=names)

# Загрузка данных в стейджинг
cursor_dwh.executemany(insert_to_db(col, base, xxxx_stg), df.values.tolist())

#Захват id из источника в smla_stg_del_accounts
id = 'account'
cursor_bank.execute(select_from_db(id, base_source, table_source))

records = cursor_bank.fetchall()
names = [x[0] for x in cursor_bank.description]
df = pd.DataFrame(records, columns=names)

# Загрузка id в stg_del
xxxx_stg_del = 'smla_stg_del_accounts'
cursor_dwh.executemany(insert_to_db('id', base, xxxx_stg_del), df.values.tolist())

#Инкриментальная загрузка в smla_dwh_dim_accounts_hist
xxxx_target = 'smla_dwh_dim_accounts_hist'
col_tgt = 'account_num, valid_to, client, effective_from, effective_to, deleted_flg'
cursor_dwh.execute(scd2_incr(base, xxxx_stg_del, xxxx_source, xxxx_target, xxxx_stg, col, col_tgt, xxxx_meta))

###############################################################
#clients
# Захват данных из источника в стейджинги
xxxx_meta = 'smla_meta_clients'
xxxx_source = 'clients'

# Выбор max_update_dt из meta_clients
cursor_dwh.execute(select_mudt_from_meta(xxxx_meta, xxxx_source))

records = cursor_dwh.fetchall()
names = [x[0] for x in cursor_dwh.description]
df = pd.DataFrame(records, columns=names)

# stg_clients
col = """client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, 
update_dt"""
base_source = 'info'
table_source = 'clients'
base = 'demipt3'
xxxx_stg = 'smla_stg_clients'

cursor_bank.execute(f"""select {col} from {base_source}.{table_source}
                    where coalesce(create_dt,update_dt) >= '{df['max_update_dt'][0]}';""")

records = cursor_bank.fetchall()
names = [x[0] for x in cursor_bank.description]
df = pd.DataFrame(records, columns=names)

# Загрузка данных в стейджинг
cursor_dwh.executemany(insert_to_db(col, base, xxxx_stg), df.values.tolist())

#Захват id из источника в smla_stg_del_clients
id = 'client_id'
cursor_bank.execute(select_from_db(id, base_source, table_source))

records = cursor_bank.fetchall()
names = [x[0] for x in cursor_bank.description]
df = pd.DataFrame(records, columns=names)

# Загрузка id в stg_del
xxxx_stg_del = 'smla_stg_del_clients'
cursor_dwh.executemany(insert_to_db('id', base, xxxx_stg_del), df.values.tolist())

#Инкриментальная загрузка в smla_dwh_dim_clients_hist
xxxx_target = 'smla_dwh_dim_clients_hist'
col_tgt = """client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, 
effective_from, effective_to, deleted_flg"""

cursor_dwh.execute(scd2_incr(base, xxxx_stg_del, xxxx_source, xxxx_target, xxxx_stg, col, col_tgt, xxxx_meta))

###############################################################
#Загрузка фактовых таблиц

#passport_blacklist

# stg_passport_blacklist
col = 'entry_dt, passport_num'
base = 'demipt3'
xxxx_stg = 'smla_stg_passport_blacklist'

# выберет все файлы заблокированных паспортов
my_f_list = [f for f in glob.iglob('/home/demipt3/smla/project/passport_blacklist_*.xlsx')]
my_f_list = sorted(my_f_list)

# загрузит первый файл(если найдет несколько файлов)
df = pd.read_excel(f'{my_f_list[0]}', sheet_name='blacklist', header=0, index_col=None)
pd_dt = my_f_list[0].split(sep='/')

# Загрузка данных в стейджинг
cursor_dwh.executemany(insert_to_db(col, base, xxxx_stg), df.values.tolist())

#dwh_fact_passport_blacklist
xxxx_target = 'smla_dwh_fact_passport_blacklist'
col = 'passport_num, entry_dt'
col_tgt = 'passport_num, entry_dt'

# Загрузка фактовой таблицы
cursor_dwh.execute(insert_to_fact_table(base, xxxx_target, xxxx_stg, col, col_tgt))
# Переименование файла и его перемещение
os.rename(f'{my_f_list[0]}', f'/home/demipt3/smla/project/archive/{pd_dt[-1]}.backup')

###############################################################
# transactions

# smla_stg_transactions
col = 'transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal'
base = 'demipt3'
xxxx_stg = 'smla_stg_transactions'

# выберет все файлы транзакций
my_f_list = [f for f in glob.iglob('/home/demipt3/smla/project/transactions_*.txt')]
my_f_list = sorted(my_f_list)

# загрузит первый файл(если найдет несколько файлов)
df = pd.read_csv(f'{my_f_list[0]}', sep=';', decimal=',')
pd_dt = my_f_list[0].split(sep='/')

# Загрузка данных в стейджинг
cursor_dwh.executemany(f""" INSERT INTO {base}.{xxxx_stg}({col})
                            VALUES(%s, to_timestamp(%s,'YYYY-MM-DD HH24:MI:SS'), %s, %s, %s, %s, %s)""", df.values.tolist())

#smla_dwh_fact_transactions
xxxx_target = 'smla_dwh_fact_transactions'
col = 'transaction_id, transaction_date, card_num, oper_type, amount, oper_result, terminal'
col_tgt = 'trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal'

# Загрузка фактовой таблицы
cursor_dwh.execute(insert_to_fact_table(base, xxxx_target, xxxx_stg, col, col_tgt))
# Переименование файла и его перемещение
os.rename(f'{my_f_list[0]}', f'/home/demipt3/smla/project/archive/{pd_dt[-1]}.backup')

###############################################################
#terminals

# Захват данных из источника в стейджинги
xxxx_source = 'terminals'

# stg_terminals
# выберет все файлы терминалов
my_f_list = [f for f in glob.iglob('/home/demipt3/smla/project/terminals_*.xlsx')]
my_f_list = sorted(my_f_list)

# загрузит первый файл(если найдет несколько файлов)
df = pd.read_excel(f'{my_f_list[0]}', sheet_name='terminals', header=0, index_col=None)
# время создания
create_dt = ((my_f_list[0].split(sep='/')[-1]).split(sep='_'))[1].split(sep='.')[0]
create_dt = datetime.datetime.strptime(f'{create_dt}', "%d%m%Y").date()

col = 'terminal_id, terminal_type, terminal_city, terminal_address'
base = 'demipt3'
xxxx_stg = 'smla_stg_terminals'

# Вставка считанных файлов в стейджинг
cursor_dwh.executemany(insert_to_db(col, base, xxxx_stg), df.values.tolist())

#smla_stg_del_terminals
xxxx_stg_del = 'smla_stg_del_terminals'
df_list = df.values.tolist()
# Столбец существующих id
id = [[row[0]] for row in df_list]
# Вставка в базу данных
cursor_dwh.executemany(insert_to_db('id', base, xxxx_stg_del), id)

#Инкриментальная загрузка в smla_dwh_dim_terminals_hist
xxxx_target = 'smla_dwh_dim_terminals_hist'
col_tgt = """terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg"""
cursor_dwh.execute(scd2_without_crdt_updt(base, xxxx_stg_del, xxxx_target, xxxx_stg, col, col_tgt, create_dt))
# Переименование файла и его перемещение
os.rename(f'{my_f_list[0]}', f'/home/demipt3/smla/project/archive/terminals_{create_dt}.backup')

# Построение отчета
cursor_dwh.execute(f"""
with selmin20 as(
select  sdft.card_num, trans_date, oper_result, amt, min0, min20 
from demipt3.smla_dwh_fact_transactions sdft
inner join 
    (select card_num, (trans_date - interval '20 minute') as min20, trans_date as min0
    from demipt3.smla_dwh_fact_transactions sdft
    where oper_result='SUCCESS') crmin20
on sdft.card_num = crmin20.card_num 
    and sdft.trans_date between to_timestamp('{create_dt}','YYYY-MM-DD') - interval '20 minute'
    and to_timestamp('{create_dt}','YYYY-MM-DD') + interval '1 day'
where (sdft.oper_type = 'PAYMENT' or sdft.oper_type= 'WITHDRAW') and (sdft.trans_date between min20 and min0))
insert into demipt3.smla_rep_fraud (
select
    trans_date as event_dt,
    passport_num as passport,
    last_name ||' '|| first_name ||' '|| patronymic as fio,
    phone,
    '1' event_type,
    to_timestamp('{create_dt}','YYYY-MM-DD') as report_dt
from 
    (select * from demipt3.smla_dwh_dim_clients_hist sddch 
    where passport_valid_to < to_timestamp('{create_dt}','YYYY-MM-DD')
          or passport_num in (select passport_num from demipt3.smla_dwh_fact_passport_blacklist sdfpb))t 
inner join demipt3.smla_dwh_dim_accounts_hist acc 
    on t.client_id = acc.client
inner join demipt3.smla_dwh_dim_cards_hist cr
    on acc.account_num  = cr.account_num 
inner join smla_dwh_fact_transactions trans
    on trans.card_num = cr.card_num 
    and(trans.trans_date  between cr.effective_from and cr.effective_to)
    and cr.deleted_flg = 'N'
    and trans.trans_date between to_timestamp('{create_dt}','YYYY-MM-DD')
    and to_timestamp('{create_dt}','YYYY-MM-DD') + interval '1 day'
union 	
select
    trans_date as event_dt,
    passport_num as passport,
    last_name ||' '|| first_name ||' '|| patronymic as fio,
    phone,
    '2' event_type,
    to_timestamp('{create_dt}','YYYY-MM-DD') as report_dt
from 
    (select * from demipt3.smla_dwh_dim_accounts_hist acc
    where valid_to < to_timestamp('{create_dt}','YYYY-MM-DD'))t
inner join demipt3.smla_dwh_dim_clients_hist cl
    on cl.client_id = t.client
inner join demipt3.smla_dwh_dim_cards_hist cr
    on t.account_num  = cr.account_num 
inner join smla_dwh_fact_transactions trans
    on trans.card_num = cr.card_num 
    and(trans.trans_date  between cr.effective_from and cr.effective_to)
    and cr.deleted_flg = 'N'
    and trans.trans_date between to_timestamp('{create_dt}','YYYY-MM-DD')
                             and to_timestamp('{create_dt}','YYYY-MM-DD') + interval '1 day'
union 
select 
    trans_date as event_dt,
    passport_num as passport,
    last_name ||' '|| first_name ||' '|| patronymic as fio,
    phone,
    '3' event_type,
    to_timestamp('{create_dt}','YYYY-MM-DD') as report_dt
from (
    select * from(
        select trans_date, card_num, terminal_city, 
            lag(terminal_city) over (partition by card_num order by  trans_date) last_city,
            lag(trans_date) over (partition by card_num order by  trans_date) last_date
        from demipt3.smla_dwh_fact_transactions sdft
        inner join demipt3.smla_dwh_dim_terminals_hist sddth
            on  sdft.terminal = sddth.terminal_id 
            and(sdft.trans_date  between sddth.effective_from and sddth.effective_to
            and sddth.deleted_flg = 'N'
            and sdft.trans_date between to_timestamp('{create_dt}','YYYY-MM-DD') - interval '1 hour'
                                    and to_timestamp('{create_dt}','YYYY-MM-DD') + interval '1 day'))t
        where terminal_city <> last_city 
        and trans_date between last_date and last_date + interval '1 hour') t2
inner join demipt3.smla_dwh_dim_cards_hist cr
    on t2.card_num = cr.card_num
    and(t2.trans_date  between cr.effective_from and cr.effective_to)
    and cr.deleted_flg = 'N'
inner join demipt3.smla_dwh_dim_accounts_hist acc 
    on cr.account_num = acc.account_num 
inner join demipt3.smla_dwh_dim_clients_hist cl
    on cl.client_id = acc.client
union 
select 
    min0 as event_dt,
    passport_num as passport,
    last_name ||' '|| first_name ||' '|| patronymic as fio,
    phone,
    '4' event_type,
    to_timestamp('{create_dt}','YYYY-MM-DD') as report_dt
from (
    select * from(
        select *, lag(amt) over (partition by selmin20.card_num ) as last_amt
        from selmin20 
        inner join 
            (select card_num as cn, min0 as min1, oper_result, count(oper_result) as cntfail
            from selmin20
            group by cn, min1, oper_result
            having oper_result='REJECT' and count(oper_result)>2) as groupmin20
        on (selmin20.card_num = groupmin20.cn) and (selmin20.min0 = groupmin20.min1)
        order by selmin20.card_num, trans_date) t
        where amt < last_amt and trans_date = min0) t2
        inner join demipt3.smla_dwh_dim_cards_hist cr
            on t2.card_num = cr.card_num 
                and t2.trans_date between cr.effective_from and cr.effective_to
                and cr.deleted_flg = 'N'
        inner join demipt3.smla_dwh_dim_accounts_hist acc 
            on cr.account_num = acc.account_num 
        inner join demipt3.smla_dwh_dim_clients_hist cl
            on cl.client_id = acc.client);

""")


conn_dwh.commit()

# Закрываем соединение
cursor_dwh.close()
cursor_bank.close()

conn_dwh.close()
conn_bank.close()
