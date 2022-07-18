import io

import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table

# crea la conexión a la base de datos, primero crea un engine que es basicamente una instancia de la base de datos
# cuando ya está instanciada se crea la conexión a la base de datos
# metadata es un objeto que contiene toda la información correspondiente a los features de la base de datos

engine = create_engine('postgresql+psycopg2://postgres:shakejunt02@localhost:5432/g9')
connection = engine.connect()
metadata = MetaData()

# inspect devuelve un objeto, en este caso se usa para ver un listado de las tablas de la DB y comprobar la conexión
insp = inspect(engine)
print(insp.get_table_names())

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
Trip_df = pd.read_csv('./tables/trip.csv', dtype={'Duration': int})
Payment_df = pd.read_csv('./tables/payment.csv')
Vendor_df = pd.read_csv('./tables/vendor.csv')
Borough_df = pd.read_csv('./tables/borough.csv')
Service_Zone_df = pd.read_csv('./tables/service_zone.csv')
Precip_Type_df = pd.read_csv('./tables/precip_type.csv')
Rate_Code_df = pd.read_csv('./tables/rate_code.csv')
Payment_Type_df = pd.read_csv('./tables/payment_type.csv')
Calendar_df = pd.read_csv('./tables/calendar.csv')
Zone_df = pd.read_csv('./tables/zone.csv')

# Carga los contenidos de los dataframes en las tablas correspondientes en la DB, se recomienda ejecutar uno por uno.
# Para las tablas con mayor número de registros se recomienda trabajar con el parametro chunksize
Service_Zone_df.to_sql('Service_Zone', engine, if_exists='append', index=False, method='multi')
Borough_df.to_sql('Borough', engine, if_exists='append', index=False, method='multi')
Zone_df.to_sql('Zone', engine, if_exists='append', index=False, method='multi')
Vendor_df.to_sql('Vendor', engine, if_exists='append', index=False, method='multi')
Calendar_df.to_sql('Calendar', engine, if_exists='append', index=False, method='multi')
Precip_Type_df.to_sql('Precip_Type', engine, if_exists='append', index=False, method='multi')
Rate_Code_df.to_sql('Rate_Code', engine, if_exists='append', index=False, method='multi')
Payment_Type_df.to_sql('Payment_Type', engine, if_exists='append', index=False, method='multi')


# Función para optimizar la carga de tablas grandes
def sql_copy_opt(df, tablename, eng):
    conn = eng.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, tablename, null="") # null values become ''
    conn.commit()
    cur.close()


Trip_df.loc[(Trip_df['IdPrecip_Type'].isna()), 'IdPrecip_Type'] = 0
Trip_df.IdPrecip_Type = [*map(round, Trip_df.IdPrecip_Type)]
sql_copy_opt(Trip_df, 'Trip', engine)
sql_copy_opt(Payment_df, 'Payment', engine)
