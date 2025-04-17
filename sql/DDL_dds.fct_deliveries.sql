-- DDL таблицы фактов (доставки)

drop table if exists dds.fct_deliveries;

create table if not exists dds.fct_deliveries (
	id serial not null primary key, -- идентификатор записи (суррогатный ключ)
	delivery_key varchar not null unique, -- бизнес-ключ доставки
	order_id int not null, -- id заказа = ссылка на pk измерения (заказ)
	timestamp_id int not null, -- timestamp доставки = ссылка на pk измерения (timestamp)
	order_sum numeric (14,2) not null, -- сумма заказа
	courier_id int not null, -- id курьера = ссылка на pk измерения (курьер)
	rating smallint not null default 0, -- рейтинг доставки
	tips numeric (14,2) not null default 0, -- чаевые
	foreign key (order_id) references dds.dm_orders (id),
	foreign key (timestamp_id) references dds.dm_timestamps (id),
	foreign key (courier_id) references dds.dm_couriers (id)
);

alter table dds.fct_deliveries add check (order_sum >= 0);
alter table dds.fct_deliveries add check (rating between 0 and 5);
alter table dds.fct_deliveries add check (tips >= 0);