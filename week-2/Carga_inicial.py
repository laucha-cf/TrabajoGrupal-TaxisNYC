import io
import os
import fnmatch

import pandas as pd
from sqlalchemy import create_engine, MetaData
import time

# -- CONSTANTES -- #
DATA_PATH = '../tables/'

DBMS = 'postgresql'
DRIVER = 'psycopg2'
USER = 'postgres'
PASSWORD = '4217796'
HOST = 'localhost'
PORT = '5432'
DB_NAME = 'nyc_taxis'

# crea la conexión a la base de datos, primero crea un engine que es basicamente una instancia de la base de datos
# cuando ya está instanciada se crea la conexión a la base de datos
# metadata es un objeto que contiene toda la información correspondiente a los features de la base de datos

engine = create_engine(f'{DBMS}+{DRIVER}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
connection = engine.connect()
metadata = MetaData()

# Se levantan los dataframes correspondientes a cada tabla
Vendor_df = pd.read_csv(DATA_PATH + 'vendor.csv')
Borough_df = pd.read_csv(DATA_PATH + 'borough.csv')
Service_Zone_df = pd.read_csv(DATA_PATH + 'service_zone.csv')
Precip_Type_df = pd.read_csv(DATA_PATH + 'precip_type.csv')
Rate_Code_df = pd.read_csv(DATA_PATH + 'rate_code.csv')
Payment_Type_df = pd.read_csv(DATA_PATH + 'payment_type.csv')
Zone_df = pd.read_csv(DATA_PATH + 'zone.csv')
# Seguimos con las tablas que soportan carga incremental.
outlier_filename = fnmatch.filter(os.listdir(DATA_PATH), 'outlier_*.csv')[0]
Outlier_df = pd.read_csv(DATA_PATH + outlier_filename)

calendar_filename = fnmatch.filter(os.listdir(DATA_PATH), 'calendar_*.csv')[0]
Calendar_df = pd.read_csv(DATA_PATH + calendar_filename)

trip_filename = fnmatch.filter(os.listdir(DATA_PATH), 'trip_*.csv')[0]
Trip_df = pd.read_csv(DATA_PATH + trip_filename, dtype={'duration': int})

payment_filename = fnmatch.filter(os.listdir(DATA_PATH), 'payment_*.csv')[0]
Payment_df = pd.read_csv(DATA_PATH + payment_filename)

# Carga los contenidos de los dataframes en las tablas correspondientes en la DB, se recomienda ejecutar uno por uno.
# Para las tablas con mayor número de registros se recomienda trabajar con el parametro chunksize
dataframes = [Service_Zone_df, Borough_df, Zone_df, Vendor_df, Calendar_df, Precip_Type_df, Rate_Code_df,
              Payment_Type_df, Trip_df, Payment_df, Outlier_df]
tables_names = ['service_zone', 'borough', 'zone', 'vendor', 'calendar', 'precip_type', 'rate_code',
                'payment_type', 'trip', 'payment', 'aux_outlier']
df_times = pd.DataFrame({'tables': tables_names})
start_tms = []
end_tms = []


def fill_table(p_name, p_dataframe):
    """
    Función para poblar las tablas mediante SQLAlchemy.

    :type p_name: str
    :param p_name: Nombre de la tabla a poblar.

    :type p_dataframe: pandas.DataFrame
    :param p_dataframe: DataFrame a insertar en la tabla.
    """
    print(f'Insertando {p_name}...')
    start = time.time()
    p_dataframe.to_sql(p_name, engine, if_exists='append', index=False, method='multi')
    start_tms.append(start)
    end_tms.append(time.time())


# Función para optimizar la carga de tablas grandes
def sql_copy_opt(df, tablename, eng):
    """
    Optimiza el tiempo de carga para tablas grandes.

    :type df: pandas.DataFrame
    :param df: DataFrame a insertar en la tabla.

    :type tablename: str
    :param tablename: Nombre de la tabla a poblar.

    :type eng: sqlalchemy.engine.base.Engine
    :param eng: Engine usada para conectarse a la DB.
    """
    start = time.time()
    conn = eng.raw_connection()
    print(f'Insertando {tablename}...')
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, tablename, null="")  # valores nulos se convierten a ''
    conn.commit()
    start_tms.append(start)
    end_tms.append(time.time())
    cur.close()


# Poblamos todas las tablas
for i, name in enumerate(tables_names):
    if name == 'trip':
        Trip_df.loc[(Trip_df['idprecip_type'].isna()), 'idprecip_type'] = 0
        Trip_df.idprecip_type = [*map(round, Trip_df.idprecip_type)]
        sql_copy_opt(Trip_df, name, engine)
    elif name == 'payment':
        sql_copy_opt(Payment_df, name, engine)
    else:      
        fill_table(name, dataframes[i])


# Anotamos los tiempos de carga
df_times['Start'] = start_tms
df_times['Stop'] = end_tms
df_times['RunTime'] = df_times['Stop'] - df_times['Start']
df_times.to_csv('TiempoEjecucion.csv')
