create table demipt3.smla_dwh_fact_transactions(
	trans_id varchar(15),
	trans_date timestamp(0),
	card_num char(20),
	oper_type varchar(10),
	amt decimal,
	oper_result varchar(10),
	terminal varchar(5));

create table demipt3.smla_dwh_fact_passport_blacklist(
	passport_num varchar(15),
	entry_dt date);

create table demipt3.smla_dwh_dim_terminals_hist(
	terminal_id varchar(5),
	terminal_type varchar(3),
	terminal_city varchar(20),
	terminal_address varchar(100),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1));

create table demipt3.smla_dwh_dim_cards_hist(
	card_num varchar(20),
	account_num varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1));

create table demipt3.smla_dwh_dim_accounts_hist(
	account_num varchar(20),
	valid_to date,
	client varchar(10),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1));

create table demipt3.smla_dwh_dim_clients_hist(
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg char(1));

---------------------------------------------------------

create table demipt3.smla_stg_transactions(
	transaction_id varchar(11),
	transaction_date timestamp(0),
	amount decimal,
	card_num varchar(20),
	oper_type varchar(10),
	oper_result varchar(10),
	terminal  varchar(5));

create table demipt3.smla_stg_passport_blacklist(
	entry_dt date,
	passport_num varchar(15));

create table demipt3.smla_stg_terminals(
	terminal_id varchar(5),
	terminal_type varchar(3),
	terminal_city varchar(20),
	terminal_address varchar(100));

create table demipt3.smla_stg_cards(
	card_num varchar(20),
	account varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0));

create table demipt3.smla_stg_accounts(
	account varchar(20),
	valid_to date,
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0));

create table demipt3.smla_stg_clients(
	client_id varchar(10),
	last_name varchar(20),
	first_name varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	create_dt timestamp(0),
	update_dt timestamp(0));

---------------------------------------------------------

create table demipt3.smla_rep_fraud(
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(65),
	phone varchar(16),
	event_type char(1),
	report_dt timestamp(0));

---------------------------------------------------------

create table demipt3.smla_stg_del_cards( 
	id varchar(20));

create table demipt3.smla_stg_del_accounts( 
	id varchar(20));

create table demipt3.smla_stg_del_clients( 
	id varchar(20));

create table demipt3.smla_stg_del_terminals( 
	id varchar(20));

---------------------------------------------------------

create table demipt3.smla_meta_cards(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0));

create table demipt3.smla_meta_accounts(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0));

create table demipt3.smla_meta_clients(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0));






