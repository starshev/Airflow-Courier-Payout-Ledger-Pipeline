-- DDL таблицы доставок в слое STG (для JSON-ответов от API системы доставок)

drop table if exists stg.deliverysystem_deliveries;

create table if not exists stg.deliverysystem_deliveries (
	id serial not null primary key, -- идентификатор записи (суррогатный ключ)
	json_response text not null, -- JSON-ответ целиком
	delivery_key varchar not null, -- бизнес-ключ доставки
	delivery_ts timestamp not null -- timestamp доставки
);

alter table stg.deliverysystem_deliveries add unique (delivery_key);