from sqlalchemy import create_engine, inspect
import sqlalchemy as sqa
import pandas as pd


## crea la conexión a la base de datos, primero crea un engine que es basicamente una instancia de la base de datos
## cuando ya está instanciada se crea la conexión a la base de datos
## metadata es un objeto que contiene toda la información correspondiente a los features de la base de datos (tablas, relaciones, etc)

engine = create_engine('postgresql+psycopg2://postgres:buried-in-smoke@henry-pg9.cyp4yswbeqvc.us-east-1.rds.amazonaws.com:5432/nyc_taxis')
connection = engine.connect()
metadata = sqa.MetaData()

## inspect devuelve un objeto, en este caso se usa para ver un listado de las tabas de la base de datos y comprobar la conexión
insp = inspect(engine)
print(insp.get_table_names())

## Asignamos a una variable la tabla leída a través de sqlalchemy par poderla manipular

Trip = sqa.Table('Trip', metadata ,autoload=True, autoload_with=engine)
Payment = sqa.Table('Payment', metadata ,autoload=True, autoload_with=engine)
Vendor = sqa.Table('Vendor', metadata ,autoload=True, autoload_with=engine)
Borough = sqa.Table('Borough', metadata ,autoload=True, autoload_with=engine)
Service_Zone = sqa.Table('Service_Zone', metadata ,autoload=True, autoload_with=engine)
Precip_Type = sqa.Table('Precip_Type', metadata ,autoload=True, autoload_with=engine)
Rate_Code = sqa.Table('Rate_Code', metadata ,autoload=True, autoload_with=engine)
Payment_Type = sqa.Table('Payment_Type', metadata ,autoload=True, autoload_with=engine)
Calendar = sqa.Table('Calendar', metadata ,autoload=True, autoload_with=engine)
Zone = sqa.Table('Zone', metadata ,autoload=True, autoload_with=engine)

## Se levantan los dataframes correspondientes a cada tabla

Payment_df=pd.read_csv('./tables/payment.csv')
Vendor_df=pd.read_csv('./tables/vendor.csv')
Borough_df=pd.read_csv('./tables/borough.csv')
Service_Zone_df=pd.read_csv('./tables/service_zone.csv')
Precip_Type_df=pd.read_csv('./tables/precip_type.csv')
Rate_Code_df=pd.read_csv('./tables/rate_code.csv')
Payment_Type_df=pd.read_csv('./tables/payment_type.csv')
Calendar_df=pd.read_csv('./tables/calendar.csv')
Zone_df=pd.read_csv('./tables/zone.csv')

## Fue necesario eliminar la primera columna de los csv porque se había incluido el índice cuando se guardaron

Payment_df.drop(columns='Unnamed: 0', inplace=True)
Vendor_df.drop(columns='Unnamed: 0', inplace=True)
Borough_df.drop(columns='Unnamed: 0', inplace=True)
Service_Zone_df.drop(columns='Unnamed: 0', inplace=True)
Precip_Type_df.drop(columns='Unnamed: 0', inplace=True)
Rate_Code_df.drop(columns='Unnamed: 0', inplace=True)
Payment_Type_df.drop(columns='Unnamed: 0', inplace=True)
Calendar_df.drop(columns='Unnamed: 0', inplace=True)
Zone_df.drop(columns='Unnamed: 0', inplace=True)


## Carga los contenidos de los dataframes en las tablas correspondientes en la base de datos, se recomienda ejecutar uno por uno.
## Para las tablas con mayor número de registros se recomienda trabajar con el parametro chunksize

Payment_df.to_sql('Payment',engine, if_exists='replace', index=False, method='multi', chunksize=50000)
Vendor_df.to_sql('Vendor',engine, if_exists='replace', index=False, method='multi')
Borough_df.to_sql('Borough',engine, if_exists='replace', index=False, method='multi')
Service_Zone_df.to_sql('Service_Zone',engine, if_exists='replace', index=False, method='multi')
Precip_Type_df.to_sql('Precip_Type',engine, if_exists='replace', index=False, method='multi')
Rate_Code_df.to_sql('Rate_Code',engine, if_exists='replace', index=False, method='multi')
Payment_Type_df.to_sql('Payment_Type',engine, if_exists='replace', index=False, method='multi')
Calendar_df.to_sql('Calendar',engine, if_exists='replace', index=False, method='multi')
Zone_df.to_sql('Zone',engine, if_exists='replace', index=False, method='multi')