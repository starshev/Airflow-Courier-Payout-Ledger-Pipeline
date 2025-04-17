-- таймстемпы, которые присутствуют в свежем инкременте по доставкам
with new_timestamps as (
	select distinct delivery_ts as ts
	from stg.deliverysystem_deliveries
	where delivery_ts > coalesce(
		(select (workflow_settings ->> 'last_loaded_ts')::timestamp
		from dds.srv_wf_settings 
		where workflow_key = 'deliveries_stg_to_dds_workflow'), '2022-01-01'::timestamp)
	),
-- загружаем их в DDS (новые - добавляем, существующие - не трогаем)
insert_timestamps as (
	insert into dds.dm_timestamps (ts, "year", "month", "day", "time", "date")
	select
		ts,
		extract(year from ts),
		extract(month from ts),
		extract(day from ts),
		ts::time,
		ts::date
	from new_timestamps
	on conflict (ts) do nothing
	)
select 'Таймстемпы успешно загружены.';