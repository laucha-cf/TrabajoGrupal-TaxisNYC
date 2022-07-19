import io

import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table
import datetime as dt
import time

# crea la conexión a la base de datos, primero crea un engine que es basicamente una instancia de la base de datos
# cuando ya está instanciada se crea la conexión a la base de datos
# metadata es un objeto que contiene toda la información correspondiente a los features de la base de datos

engine = create_engine('postgresql+psycopg2://postgres:shakejunt02@localhost:5432/g9')
connection = engine.connect()
metadata = MetaData()

# inspect devuelve un objeto, en este caso se usa para ver un listado de las tablas de la DB y comprobar la conexión
insp = inspect(engine)

# Asignamos a una variable la tabla leída a través de sqlalchemy par poder manipularla
Trip = Table('Trip', metadata, autoload=True, autoload_with=engine)
Payment = Table('Payment', metadata, autoload=True, autoload_with=engine)
Vendor = Table('Vendor', metadata, autoload=True, autoload_with=engine)
Borough = Table('Borough', metadata, autoload=True, autoload_with=engine)
Service_Zone = Table('Service_Zone', metadata, autoload=True, autoload_with=engine)
Precip_Type = Table('Precip_Type', metadata, autoload=True, autoload_with=engine)
Rate_Code = Table('Rate_Code', metadata, autoload=True, autoload_with=engine)
Payment_Type = Table('Payment_Type', metadata, autoload=True, autoload_with=engine)
Calendar = Table('Calendar', metadata, autoload=True, autoload_with=engine)
Zone = Table('Zone', metadata, autoload=True, autoload_with=engine)

# Se levantan los dataframes correspondientes a cada tabla
Trip_df = pd.read_csv('../tables/trip.csv', dtype={'Duration': int})
Payment_df = pd.read_csv('../tables/payment.csv')
Vendor_df = pd.read_csv('../tables/vendor.csv')
Borough_df = pd.read_csv('../tables/borough.csv')
Service_Zone_df = pd.read_csv('../tables/service_zone.csv')
Precip_Type_df = pd.read_csv('../tables/precip_type.csv')
Rate_Code_df = pd.read_csv('../tables/rate_code.csv')
Payment_Type_df = pd.read_csv('../tables/payment_type.csv')
Calendar_df = pd.read_csv('../tables/calendar.csv')
Zone_df = pd.read_csv('../tables/zone.csv')


# Carga los contenidos de los dataframes en las tablas correspondientes en la DB, se recomienda ejecutar uno por uno.
# Para las tablas con mayor número de registros se recomienda trabajar con el parametro chunksize
dataframes = [Service_Zone_df, Borough_df, Zone_df, Vendor_df, Calendar_df, Precip_Type_df, Rate_Code_df, Payment_Type_df, Trip_df, Payment_df]
tables_names = ['Service_Zone', 'Borough', 'Zone', 'Vendor', 'Calendar', 'Precip_Type', 'Rate_Code', 'Payment_Type', 'Trip', 'Payment']
df_times = pd.DataFrame({'tables': tables_names})
start_tms = []
end_tms = []

def fill_table( p_name, p_dataframe ):
    '''Función para poblar las tablas mediante SQLAlchemy
    param: p_name String ->Nombre de la tabla
    param: p_dataframe DataFrame ->DataFrame de Datos
    '''
    print(f'Insertando {p_name}...')
    start = time.time()
    p_dataframe.to_sql(p_name, engine, if_exists='append', index=False, method='multi')
    start_tms.append(start)
    end_tms.append(time.time())
    
# Función para optimizar la carga de tablas grandes
def sql_copy_opt(df, tablename, eng):
    '''
    Optimiza la carga de tablas grandes
    param: df DataFrame
    param: tablename String
    param: eng (insertar tipo)
    '''
    conn = eng.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, tablename, null="") # null values become ''
    conn.commit()
    cur.close()

# Tablas Grandes
def fill_Trip():
    '''Puebla la tabla Trip
    '''
    Trip_df.loc[(Trip_df['IdPrecip_Type'].isna()), 'IdPrecip_Type'] = 0
    Trip_df.IdPrecip_Type = [*map(round, Trip_df.IdPrecip_Type)]
    print(f'Insertando Trip...')
    start = time.time()
    sql_copy_opt(Trip_df, 'Trip', engine)
    start_tms.append(start)
    end_tms.append(time.time()) 
def fill_Payment():
    '''Puebla la tabla Payment
    '''
    print('Insertando Payment...')
    start = time.time()
    sql_copy_opt(Payment_df, 'Payment', engine)
    start_tms.append(start)
    end_tms.append(time.time()) 

# Poblamos todas las tablas
for i, name in enumerate(tables_names):
    if name=='Trip':
        fill_Trip()
    elif name=='Payment':
        fill_Payment()
    else:      
        fill_table( name, dataframes[i] )

#Cerramos la conexión y la sesión con la base de datos
connection.close()
engine.dispose()
# Anotamos los tiempos de carga
df_times['Start'] = start_tms
df_times['Stop'] = end_tms
df_times['RunTime'] = df_times['Stop'] - df_times['Start']
df_times.to_csv('TiempoEjecucion.csv')