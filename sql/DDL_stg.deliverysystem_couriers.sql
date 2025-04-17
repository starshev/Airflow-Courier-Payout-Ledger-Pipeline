-- DDL таблицы курьеров в слое STG (для JSON-ответов от API системы доставок)

drop table if exists stg.deliverysystem_couriers;

create table if not exists stg.deliverysystem_couriers (
	id serial not null primary key, -- идентификатор записи (суррогатный ключ)
	json_response text not null, -- JSON-ответ целиком
	courier_key varchar not null -- бизнес-ключ доставки
);

alter table stg.deliverysystem_couriers add unique (courier_key);