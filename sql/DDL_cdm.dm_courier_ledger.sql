-- DDL витрины для расчётов с курьерами

drop table if exists cdm.dm_courier_ledger;

create table if not exists cdm.dm_courier_ledger (
	id serial not null primary key, -- идентификатор записи (суррогатный ключ)
	courier_id varchar not null, -- id курьера, которому перечисляем
	courier_name varchar not null, -- Ф.И.О. курьера
	settlement_year smallint not null, -- год отчёта
	settlement_month smallint not null, -- месяц отчёта
	orders_count int not null default 0, -- количество заказов за период (месяц)
	orders_total_sum numeric (14,2) not null default 0, -- общая стоимость заказов за период (месяц)
	rate_avg numeric (3,2) not null default 0, -- средний рейтинг курьера по оценкам пользователей
	order_processing_fee numeric (14,2) not null default 0, -- сумма, удержанная компанией за обработку заказов
	courier_order_sum numeric (14,2) not null default 0, -- сумма, которую необходимо перечислить курьеру
	courier_tips_sum numeric (14,2) not null default 0, -- сумма, которую пользователи оставили курьеру в качестве чаевых
	courier_reward_sum numeric (14,2) not null default 0 -- итоговая сумма, которую необходимо перечислить курьеру
);

alter table cdm.dm_courier_ledger add check (settlement_year between 2022 and 2100);
alter table cdm.dm_courier_ledger add check (settlement_month between 1 and 12);
alter table cdm.dm_courier_ledger add check (orders_count >= 0);
alter table cdm.dm_courier_ledger add check (orders_total_sum >= 0);
alter table cdm.dm_courier_ledger add check (rate_avg between 0 and 5);
alter table cdm.dm_courier_ledger add check (order_processing_fee >= 0);
alter table cdm.dm_courier_ledger add check (courier_order_sum >= 0);
alter table cdm.dm_courier_ledger add check (courier_tips_sum >= 0);
alter table cdm.dm_courier_ledger add check (courier_reward_sum >= 0);
alter table cdm.dm_courier_ledger add unique (courier_id, settlement_year, settlement_month);