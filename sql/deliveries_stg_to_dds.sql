-- таблица новых доставок из свежего инкремента из системы доставок
with new_deliveries as (
	select
		(json_response::json ->> 'delivery_id')::varchar as delivery_key,
		(json_response::json ->> 'order_id')::varchar as order_key,
		delivery_ts::timestamp as ts,
		(json_response::json ->> 'sum')::numeric(14,2) as order_sum,
		(json_response::json ->> 'courier_id')::varchar as courier_key,
		(json_response::json ->> 'rate')::int2 as rating,
		(json_response::json ->> 'tip_sum')::numeric(14,2) as tips
	from stg.deliverysystem_deliveries
	-- фильтруем по последней обработанной дате доставки из сервисной таблицы
	where delivery_ts > coalesce(
		(select (workflow_settings ->> 'last_loaded_ts')::timestamp
		from dds.srv_wf_settings 
		where workflow_key = 'deliveries_stg_to_dds_workflow'), '2022-01-01'::timestamp)
	),
-- запоминаем курсор последней обработанной даты (для обновления сервисной таблицы)
ts_cursor as (
	select max(ts) as last_loaded_ts from new_deliveries
	),
-- та же таблица новых доставок, но по структуре идентичная таблице фактов dds.fct_deliveries
new_delivery_facts as (
	select
		nd.delivery_key,
		dmo.id as order_id,
		dmt.id as timestamp_id,
		nd.order_sum,
		dmc.id as courier_id,
		nd.rating,
		nd.tips
	from new_deliveries nd
	inner join dds.dm_orders dmo on nd.order_key = dmo.order_key
	inner join dds.dm_timestamps dmt on nd.ts = dmt.ts
	inner join dds.dm_couriers dmc on nd.courier_key = dmc.courier_key
	),
-- загружаем новые доставки в DDS
new_delivery_facts_insert as (
	insert into dds.fct_deliveries (delivery_key, order_id, timestamp_id, order_sum, courier_id, rating, tips)
	select delivery_key, order_id, timestamp_id, order_sum, courier_id, rating, tips
	from new_delivery_facts
	on conflict (delivery_key) do nothing
	),
-- сохраняем курсор последней обработанной даты в сервисную таблицу
save_ts_cursor as (
	insert into dds.srv_wf_settings (
		workflow_key,
		workflow_settings
	)
	select 
		'deliveries_stg_to_dds_workflow',
		jsonb_build_object('last_loaded_ts', last_loaded_ts::timestamp)
    from ts_cursor
    where last_loaded_ts is not null
	on conflict (workflow_key) do update 
    set workflow_settings = EXCLUDED.workflow_settings
	)
select 'Новые факты доставок успешно загружены.';