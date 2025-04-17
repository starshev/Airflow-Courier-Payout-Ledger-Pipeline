-- подсчитывем основные показатели для витрины cdm.dm_courier_ledger
with ledger_main as (
	select 
		del.courier_id as courier_id,
		dmc.courier_name as courier_name,
		dmt.year as settlement_year,
		dmt.month as settlement_month,
		count(del.order_id) as orders_count,
		sum(del.order_sum) as orders_total_sum,
		avg(del.rating) filter (where del.rating between 1 and 5) as rate_avg,
		sum(del.order_sum) * 0.25 as order_processing_fee,
		sum(tips) as courier_tips_sum
	from dds.fct_deliveries del
	inner join dds.dm_couriers dmc 
		on del.courier_id = dmc.id
	inner join dds.dm_orders dmo 
		on del.order_id = dmo.id
	inner join dds.dm_timestamps dmt 
		on dmo.timestamp_id = dmt.id
	group by 
		del.courier_id,
		dmc.courier_name,
		dmt.year,
		dmt.month
	),
-- добавляем подсчет выплат курьерам
ledger_main_update1 as (
	select
		*,
		case
			when rate_avg < 4 
				then orders_total_sum * 0.05
			when rate_avg < 4.5 and rate_avg >= 4 
				then orders_total_sum * 0.07
			when rate_avg < 4.9 and rate_avg >=4.5 
				then orders_total_sum * 0.08
			when rate_avg >= 4.9 
				then orders_total_sum * 0.1
		end as courier_order_sum
	from ledger_main
	),
-- проверка на соответствие выплат курьерам минимальному порогу выплат
ledger_main_update2 as (
	select
		courier_id, 
		courier_name, 
		settlement_year, 
		settlement_month, 
		orders_count, 
		orders_total_sum, 
		rate_avg, 
		order_processing_fee,
		courier_tips_sum,
		case
			when rate_avg < 4 and courier_order_sum < 100 * orders_count 
				then 100 * orders_count 
			when rate_avg < 4.5 and rate_avg >= 4 and courier_order_sum < 150 * orders_count 
				then 150 * orders_count 
			when rate_avg < 4.9 and rate_avg >=4.5 and courier_order_sum < 175 * orders_count 
				then 175 * orders_count 
			when rate_avg >= 4.9 and courier_order_sum < 200 * orders_count 
				then 200 * orders_count 
					else courier_order_sum
		end as courier_order_sum
	from ledger_main_update1
	),
-- добавляем финальные суммы выплат
ledger_main_final as (
	select 
		*,
		courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
	from ledger_main_update2
	),
-- обновляем витрину
update_courier_ledger as (
	insert into cdm.dm_courier_ledger (
		courier_id, 
		courier_name, 
		settlement_year, 
		settlement_month, 
		orders_count, 
		orders_total_sum, 
		rate_avg, 
		order_processing_fee,
		courier_order_sum,
		courier_tips_sum,
		courier_reward_sum
		)
	select
		courier_id, 
		courier_name, 
		settlement_year, 
		settlement_month, 
		orders_count, 
		orders_total_sum, 
		rate_avg, 
		order_processing_fee,
		courier_order_sum,
		courier_tips_sum,
		courier_reward_sum
	from ledger_main_final
	on conflict (courier_id, settlement_year, settlement_month) do update
	set
		courier_name = excluded.courier_name,
		orders_count = excluded.orders_count,
		orders_total_sum = excluded.orders_total_sum,
		rate_avg = excluded.rate_avg,
		order_processing_fee = excluded.order_processing_fee,
		courier_order_sum = excluded.courier_order_sum,
		courier_tips_sum = excluded.courier_tips_sum,
		courier_reward_sum = excluded.courier_reward_sum
	)
select 'Отчет по выплатам курьерам успешно обновлен.';