from extraccion_data_football import get_data,data_processing
import pandas as pd
import time
from datetime import datetime,timedelta
from airflow.models import Variable
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import os


default_argument = {
                        'owener':'Fernando',
                        'email':'fernando.adrian1405@gmail.com',
                        'retries':1,
                        'retry_delay':timedelta(minutes=5)}

with DAG('FOOTBAL_LEAGUES',
         default_args = default_argument,
         description = 'Extrae datios de footbal',
         start_date = datetime(2022,9,21),
         schedule_interval = None,
         tags = ['tabla_espn'],
         catchup = False
         ) as dag :

        params_info = Variable.get("feature_info", deserialize_json = True)
        df = pd.read_csv('/usr/local/airflow/df_ligas.csv')
        df_team = pd.read_csv('/usr/local/airflow/team_table.csv')
        
        def extract_info(df, df_team, **kwargs):
            df_data = data_processing(df,df_team)


        extract_data = PythonOperator(task_id='EXTRACT_FOTBALL_DATA',
                                      provide_context=True,
                                      python_callable=extract_info,
                                      op_kwargs={"df":df,"df_team":df_team})
        
        upload_stage = SnowflakeOperator(task_id = 'upload_data_stage',
                                         sql='./queries/upload_stage.sql',
                                         snowflake_conn_id='demo',
                                         warehouse=params_info["DWH"],
                                         database=params_info["DB"],
                                         role=params_info["ROLE"],
                                         params=params_info)
        

        ingest_table = SnowflakeOperator(task_id = 'INGESTA_TABLA',
                                         sql='./queries/upload_table.sql',
                                         snowflake_conn_id='demo',
                                         warehouse=params_info["DWH"],
                                         database=params_info["DB"],
                                         role=params_info["ROLE"],
                                         params=params_info)
        
        extract_data >> upload_stage >> ingest_table
