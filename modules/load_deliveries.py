from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task
from airflow.models import Variable
import requests
import json
import time

endpoint = Variable.get('delivery_system_api_endpoint_deliveries')   # получаем эндпоинт API
params = {   # задаём стартовые параметры запроса к API
    'sort_field' : 'date',   # сортируем по дате доставки
    'sort_direction' : 'asc',    # сортируем по возрастанию (даты доставки)
    'limit' : 50,   # в каждом запросе - порция из 100 записей
    'offset' : 0   # на старте двигаемся с самой первой записи за указанный период
    }
headers = {   # получаем заголовки для API
    'X-Nickname' : Variable.get('delivery_system_api_nickname'),
    'X-API-KEY': Variable.get('secret_delivery_system_api_key')
    }
pg_hook = PostgresHook(postgres_conn_id = 'PG_WAREHOUSE_CONNECTION')   # получаем конфигурацию подключения к DWH (Postgres)

@task()
def load_deliveries(**kwargs):

    ds = kwargs['ds']   # получаем дату прогона дага
        
    # получаем дату последней записи в STG из сервисной таблицы;
    # если метка отсутствует, запросим данные за последние 7 дней от даты прогона дага
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('''
                        select coalesce(
                        (select (workflow_settings ->> 'last_loaded_ts')::timestamp from stg.srv_wf_settings
                        where workflow_key = 'deliverysystem_origin_to_stg_workflow'), 
                        (%(ds)s::date - interval '7 days')::timestamp
                        );
                        ''', {'ds' : ds})
            result = cur.fetchone()   # получаем строку с датой
            max_date = result[0].strftime('%Y-%m-%d %H:%M:%S')   # преобразуем полученную дату в формат, подходящий для API

    params_run = params.copy()   # копия параметров для текущего прогона 
    params_run['from'] = max_date   # полученную дату ставим в параметры API как начало периода для запроса
    params_run['to'] = f'{ds} 00:00:00'   # верхней границей времени ставим начало сегодняшнего дня
    fresh_deliveries = []   # общий список, куда складываем все порции новых записей

    for i in range(200):   # ограничиваем выгрузку 200*50 = 10000 записями за прогон
        response = requests.get(endpoint, params = params_run, headers = headers)   # ответ с порцией записей от API
        portion = response.json()   # порция записей как список словарей
        fresh_deliveries.extend(portion)   # добавляем порцию в общий список 
        if len(portion) < 50:   # если в порции < 50 записей, значит она последняя на сегодня
            break
        else:
            params_run['offset']+=50   # сдвигаем курсор на следующую порцию из 50 записей
        time.sleep(5)   # спим 5 секунд, на случай если у API есть ограничение по кол-ву запросов
        
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            for record in fresh_deliveries:   # записываем каждую новую запись в бд
                json_record = json.dumps(record, ensure_ascii = False)   # JSON-ответ каждой записи как словарь
                cur.execute('''
                            insert into stg.deliverysystem_deliveries ("json_response", "delivery_key", "delivery_ts")
                            values (%s::text, %s, %s)
                            on conflict ("delivery_key") do nothing;
                            ''',
                            (json_record, record['delivery_id'], record['delivery_ts']))

            # проверяем, что в таблице доставок есть хотя бы 1 запись (гарантия успешного прогона на старте)
            cur.execute('''
                        select count(*) from stg.deliverysystem_deliveries;
                        ''')
            if cur.fetchone()[0] > 0:
                # записываем дату последней обработанной доставки в сервисную таблицу
                cur.execute('''
                        insert into stg.srv_wf_settings (workflow_key, workflow_settings)
                        values ('deliverysystem_origin_to_stg_workflow', jsonb_build_object('last_loaded_ts', 
                        (select max(delivery_ts) from stg.deliverysystem_deliveries)
    	                )
                        )
                        on conflict (workflow_key) do update 
                        set workflow_settings = excluded.workflow_settings;
                        ''')