import time
import datetime as dt
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
import sys

sys.path.insert(1, 'C:/Users/Laucha/Desktop/TrabajoGrupal-TaxisNYC/week-2')
import Extract 
import DataCleaning 
import ddl1_tables 
import Segmentacion_Tablas 
import Carga_Inicial

default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'start_date':dt.datetime(2022,7,26)
}

dag = DAG(
    "pipeline_dag",
    default_args=default_args,
    schedule_interval=dt.timedelta(days=1),
    description='Pipeline Process',
)

extract = PythonOperator(
    task_id='Extract_Data',
    python_callable=Extract,
    dag=dag,
)

clean = PythonOperator(
    task_id='Clean_Data',
    python_callable=DataCleaning,
    dag=dag,
)

segment = PythonOperator(
    task_id='Segment_Data',
    python_callable=Segmentacion_Tablas,
    dag=dag,
)

create_db = PythonOperator(
    task_id='Create_Database',
    python_callable=ddl1_tables,
    dag=dag,
)

load = PythonOperator(
    task_id='Load_Data',
    python_callable=Carga_Inicial,
    dag=dag,
)

#Define Execution Order    
extract > clean > segment > create_db > load