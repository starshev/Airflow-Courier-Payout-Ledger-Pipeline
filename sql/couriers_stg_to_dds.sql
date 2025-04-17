-- курьеры, которые присутствуют в свежем инкременте по доставкам
with actual_couriers as (
	select distinct 
		(json_response::json ->> 'courier_id') as courier_key
	from stg.deliverysystem_deliveries
	where delivery_ts > coalesce(
		(select (workflow_settings ->> 'last_loaded_ts')::timestamp
		from dds.srv_wf_settings 
		where workflow_key = 'deliveries_stg_to_dds_workflow'), '2022-01-01'::timestamp)
	),
-- они же, но с подтянутым Ф.И.О.
actual_couriers_list as (
	select 
		ac.courier_key, 
		dsc.json_response::json ->> 'name' as courier_name
	from actual_couriers ac
	inner join stg.deliverysystem_couriers dsc
	on ac.courier_key = dsc.courier_key
	),
-- загружаем курьеров в DDS (новых - добавляем, у существующих - перезаписываем Ф.И.О.)
insert_couriers as (
	insert into dds.dm_couriers (courier_key, courier_name)
	select courier_key, courier_name
	from actual_couriers_list
	on conflict (courier_key) do update
	set courier_name = excluded.courier_name
)
select 'Курьеры успешно загружены.';