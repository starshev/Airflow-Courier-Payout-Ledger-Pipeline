-- DDL таблицы измерения (курьеры)

drop table if exists dds.dm_couriers;

create table if not exists dds.dm_couriers (
	id serial not null primary key, -- идентификатор записи (суррогатный ключ)
	courier_key varchar not null, -- бизнес-ключ курьера
	courier_name varchar not null -- Ф.И.О. курьера
);