import pandas as pd
import numpy as np
import pathlib
import requests
from io import StringIO
import boto3

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2022, 12, 1),
    "end_date": datetime.today() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'is_paused_upon_creation': False,
}


def __get_dynamic_params(date, **kwargs):
    print("TASK-1")
    url = f"https://iss.moex.com//iss/history/engines/stock/zcyc.json?date={date}"
    ti = kwargs['ti']
    print(url)
    r = requests.get(url)
    if r.status_code == 200:
        data = r.json()['params']["data"]
        if data:
            data = data[-1]
            data.pop(1)
            ti.xcom_push("dynamic_params", data)
        else:
            print("!!!!!!!!!!!! Нет данных")


def insert_dynamic_params(**kwargs):
    ti = kwargs['ti']
    params = ti.xcom_pull(key="dynamic_params")
    if params:
        df = pd.DataFrame([params])
        pg_hook = PostgresHook.get_hook("pg_conn")
        engine = pg_hook.get_sqlalchemy_engine()
        df.columns = ["dt", "betha_0", "betha_1", "betha_2", "theta", "g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8",
                      "g9"]
        print("После колонок:__________________________________________________________________")
        row_count = df.to_sql("dynamic_params", engine, schema="recost", if_exists='append', index=False)
        print(f'{row_count} rows was inserted')
        """
        pg_hook = PostgresHook(
            postgres_conn_id='pg_conn',
            schema='recost'
        )
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(sql_stmt)
        return cursor.fetchall()    
        """
    else:
        print("DF is empty-----------------------------------------------")


def __get_history_data(date):
    link = 'http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities.json'
    params = {
        'start': 1,
        'date': date
    }
    while True:
        r = requests.get(link, params)
        history = r.json()['history']
        data = history['data']
        if not data and params['start'] == 1:
            break
        elif not data and params['start'] != 1:
            break
        else:
            tmp = pd.DataFrame(data)
            original_columns = history['columns']
            tmp.columns = [x.lower() for x in original_columns]
            pg_hook = PostgresHook.get_hook("pg_conn")
            engine = pg_hook.get_sqlalchemy_engine()
            columns = [
                "TRADEDATE", "BOARDID", "SHORTNAME", "SECID", "NUMTRADES", "VALUE", "LOW", "HIGH", "CLOSE", "OPEN",
                "LEGALCLOSEPRICE", "ACCINT", "YIELDCLOSE", "VOLUME", "MARKETPRICE2", "MARKETPRICE3", "MATDATE",
                "DURATION", "COUPONPERCENT", "COUPONVALUE", "LASTTRADEDATE", "FACEVALUE", "CURRENCYID", "YIELDTOOFFER",
                "YIELDLASTCOUPON", "OFFERDATE", "FACEUNIT",
            ]
            new_columns = [x.lower() for x in columns]
            df = tmp[new_columns]
            row_count = df.to_sql("history_bonds", engine, schema="recost", if_exists='append', index=False)
            params['start'] += 100

def __get_market_data(date):
    link = "http://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
    params = {
        'iss.only': 'marketdata',
        'marketdata.columns': 'BOARDID,SECID,BID,OFFER'
    }
    r = requests.get(link, params=params)
    jsn = r.json()['marketdata']
    if jsn['data']:
        df = pd.DataFrame(jsn['data'])
        columns = jsn['columns']
        df.columns = [x.lower() for x in columns]
        df["tradedate"] = date
        pg_hook = PostgresHook.get_hook("pg_conn")
        engine = pg_hook.get_sqlalchemy_engine()
        row_count = df.to_sql("marketdata_bonds", engine, schema="recost", if_exists='append', index=False)
        print(row_count)

def __get_gspread_data(date):
    link = 'http://iss.moex.com/iss/history/engines/stock/markets/bonds/yields.json'
    params = {
        'start': 1,
        'date': date,
        'history_yields.columns': 'TRADEDATE,BOARDID,SECID,PRICE,GSPREADBP'
    }

    while True:
        r = requests.get(link, params)
        history = r.json()['history_yields']
        data = history['data']
        if not data and params['start'] == 1:
            break
        elif not data and params['start'] != 1:
            break
        else:
            df = pd.DataFrame(data)
            original_columns = history['columns']
            df.columns = [x.lower() for x in original_columns]
            pg_hook = PostgresHook.get_hook("pg_conn")
            engine = pg_hook.get_sqlalchemy_engine()
            row_count = df.to_sql("gspread", engine, schema="recost", if_exists='append', index=False)
            params['start'] += 100


def get_params_data(date):
    link = "http://iss.moex.com/iss/engines/stock/markets/bonds/securities.json"
    params = {
        'iss.only': 'securities',
        'securities.columns': 'BOARDID,SECID,ISIN,REGNUMBER,COUPONPERIOD,NEXTCOUPON,ISSUESIZE'
    }
    r = requests.get(link, params)
    print(r.url)
    securities = r.json()['securities']
    data = securities['data']
    if data:
        df = pd.DataFrame(data)
        columns = securities['columns']
        df.columns = [x.lower() for x in columns]
        df["dt"] = date
        #df['nextcoupon'] = df['nextcoupon'].apply(lambda x: x.replace('0000-00-00', ''))
        df["nextcoupon"].loc[df["nextcoupon"] == '0000-00-00'] = None
        pg_hook = PostgresHook.get_hook("pg_conn")
        engine = pg_hook.get_sqlalchemy_engine()
        row_count = df.to_sql("security_bonds", engine, schema="recost", if_exists='append', index=False)
        print(row_count)

def __result_file(date):
    pg_hook = PostgresHook.get_hook("pg_conn")
    engine = pg_hook.get_sqlalchemy_engine()
    query_count = "SELECT COUNT(DISTINCT tradedate) FROM recost.history_bonds"
    count_df = pd.read_sql(sql=query_count, con=engine)
    count = count_df.iloc[0, 0]
    if count >= 25:
        query = "SELECT * FROM recost.daily_recost dr"
        df = pd.read_sql(sql=query, con=engine)
        query_dynamic = f"SELECT * FROM recost.dynamic_params WHERE dt = '{date}'"
        df_dynamic = pd.read_sql(sql=query_dynamic, con=engine)

        #str_dt = "".join(date.split("-"))
        with pd.ExcelWriter(f"/smb/share/{date}.xlsx", engine='xlsxwriter', datetime_format='dd.mm.yyyy') as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)
            df_dynamic.to_excel(writer, sheet_name='Sheet2', index=False)
        with pd.ExcelWriter(f"/smb/share/result.xlsx", engine='xlsxwriter', datetime_format='dd.mm.yyyy') as writer:
            df.to_excel(writer, sheet_name='Sheet1', index=False)
            df_dynamic.to_excel(writer, sheet_name='Sheet2', index=False)



with DAG(
        "etl_recost_get_data",
        default_args=default_args,
        schedule_interval="50 2 * * *",
        catchup=True,
        max_active_runs=1
        # template_searchpath="/tmp"
) as dag:
    t_get_dynamic_params = PythonOperator(
        task_id="t_get_dynamic_params",
        python_callable=__get_dynamic_params,
        op_kwargs={
            "date": "{{ds}}"
        }
    )

    t_insert_dynamic_params = PythonOperator(
        task_id="t_insert_dynamic_params",
        python_callable=insert_dynamic_params,
    )

    t_get_history_data = PythonOperator(
        task_id="t_get_history_data",
        python_callable=__get_history_data,
        op_kwargs={
            "date": "{{ds}}"
        }
    )

    t_get_params_data = PythonOperator(
        task_id="t_get_params_data",
        python_callable=get_params_data,
        op_kwargs={
            "date": "{{ds}}"
        }
    )

    t_get_market_data = PythonOperator(
        task_id="t_get_market_data",
        python_callable=__get_market_data,
        op_kwargs={
            "date": "{{ds}}"
        }
    )

    t_get_gspread_data = PythonOperator(
        task_id="t_get_gspread_data",
        python_callable=__get_gspread_data,
        op_kwargs={
            "date": "{{ds}}"
        }
    )

    t_result_file = PythonOperator(
        task_id="t_result_file",
        python_callable=__result_file,
        op_kwargs={
            "date": "{{ds}}"
        }
    )





# https://moex-files.storage.yandexcloud.net/20221201.csv

t_get_dynamic_params \
>> t_insert_dynamic_params \
>> t_get_history_data \
>> t_get_params_data \
>> t_get_market_data \
>> t_get_gspread_data \
>> t_result_file
"""
link = 'http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities.json'
df = pd.DataFrame()
while self.days > 0:
    self.params['start'] = 1
    while True:
        r = requests.get(link, self.params)
        data = r.json()['history']['data']
        if not data and self.params['start'] == 1:
            self.params['date'] = minus_day(self.params['date'])
            break
        elif not data and self.params['start'] != 1:
            self.params['date'] = minus_day(self.params['date'])
            self.days -= 1
            break
        else:
            df = pd.concat([df, pd.DataFrame(data)])
            self.params['start'] += 100
    print(self.days)

df.columns = r.json()['history']['columns']

dates_columns = ['TRADEDATE', 'MATDATE', 'OFFERDATE', 'BUYBACKDATE', 'LASTTRADEDATE']
df = df.apply(lambda x: pd.to_datetime(x) if x.name in dates_columns else x)
float_columns = ['IRICPICLOSE', 'BEICLOSE', 'COUPONPERCENT', 'CBRCLOSE', 'YIELDLASTCOUPON']
df = df.apply(lambda x: pd.to_numeric(x) if x.name in float_columns else x)
df['ISDEALS'] = np.where(df['NUMTRADES'] > 0, 1, 0)

"""
