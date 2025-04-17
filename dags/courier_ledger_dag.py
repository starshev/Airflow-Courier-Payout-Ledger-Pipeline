from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from modules.load_couriers import load_couriers
from modules.load_deliveries import load_deliveries
from pathlib import Path
import pendulum

@dag(
    schedule_interval = '15 0 * * *',
    start_date = pendulum.datetime(2025, 4, 13, tz = "UTC"),
    catchup = False,
    is_paused_upon_creation = False
    )
def courier_ledger_dag():

    couriers_origin_to_stg = load_couriers()   # логика реализована в модуле load_couriers.py

    deliveries_origin_to_stg = load_deliveries()   # логика реализована в модуле load_deliveries.py

    # загружаем новых курьеров в DDS
    couriers_stg_to_dds = PostgresOperator(task_id = "couriers_stg_to_dds", 
                                        postgres_conn_id = "PG_WAREHOUSE_CONNECTION",
                                        sql = Path('/lessons/dags/sql/couriers_stg_to_dds.sql').read_text())
    
    # загружаем новые таймстемпы в DDS
    timestamps_stg_to_dds = PostgresOperator(task_id = "timestamps_stg_to_dds", 
                                        postgres_conn_id = "PG_WAREHOUSE_CONNECTION",
                                        sql = Path('/lessons/dags/sql/timestamps_stg_to_dds.sql').read_text())
    
    # загружаем новые доставки в DDS
    deliveries_stg_to_dds = PostgresOperator(task_id = "deliveries_stg_to_dds", 
                                        postgres_conn_id = "PG_WAREHOUSE_CONNECTION",
                                        sql = Path('/lessons/dags/sql/deliveries_stg_to_dds.sql').read_text())

    # дополняем витрину расчётов с курьерами в CDM
    courier_ledger_update = PostgresOperator(task_id = "update_courier_ledger", 
                                        postgres_conn_id = "PG_WAREHOUSE_CONNECTION",
                                        sql = Path('/lessons/dags/sql/courier_ledger_update.sql').read_text())

    # пайплайн
    couriers_origin_to_stg >> deliveries_origin_to_stg >> couriers_stg_to_dds >> timestamps_stg_to_dds
    timestamps_stg_to_dds >> deliveries_stg_to_dds >> courier_ledger_update

courier_ledger_dag = courier_ledger_dag()