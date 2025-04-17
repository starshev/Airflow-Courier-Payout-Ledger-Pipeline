from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task
from airflow.models import Variable
import requests
import json
import time

endpoint = Variable.get('delivery_system_api_endpoint_couriers')   # получаем эндпоинт API
params = {   # задаём стартовые параметры запроса к API
    'sort_field' : 'name',   # сортируем по дате доставки
    'sort_direction' : 'asc',    # сортируем по возрастанию (даты доставки)
    'limit' : 50,   # в каждом запросе - порция из 50 записей
    'offset' : 0   # на старте двигаемся с самой первой записи за указанный период
    }
headers = {   # получаем заголовки для API
    'X-Nickname' : Variable.get('delivery_system_api_nickname'),
    'X-API-KEY': Variable.get('secret_delivery_system_api_key')
    }
pg_hook = PostgresHook(postgres_conn_id = 'PG_WAREHOUSE_CONNECTION')   # получаем конфигурацию подключения к DWH (Postgres)

@task()
def load_couriers(**kwargs):

    ds = kwargs['ds']   # получаем дату прогона дага

    params_run = params.copy()   # копия параметров для текущего прогона
    couriers = []   # общий список, куда складываем все порции новых записей

    for i in range(200):   # ограничиваем выгрузку 200*50 = 10000 записями за прогон, для защиты от возможных неполадок в API
        response = requests.get(endpoint, params = params_run, headers = headers)   # ответ с порцией записей от API
        portion = response.json()   # порция записей как список словарей
        couriers.extend(portion)   # добавляем порцию в общий список 
        if len(portion) < 50:   # если в порции < 50 записей, значит она последняя
            break
        else:
            params_run['offset']+=50   # сдвигаем курсор на следующую порцию из 50 записей
        time.sleep(5)   # спим 5 секунд, на случай если у API есть ограничение по кол-ву запросов
        
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for record in couriers:   # записываем каждую запись в бд
                json_record = json.dumps(record, ensure_ascii = False)   # JSON-ответ каждой записи как словарь
                cur.execute('''
                            insert into stg.deliverysystem_couriers ("json_response", "courier_key")
                            values (%s::text, %s)
                            on conflict ("courier_key") do update
                            set "json_response" = excluded."json_response";
                            ''',
                            (json_record, record['_id']))