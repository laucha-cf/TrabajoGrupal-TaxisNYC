import pandas as pd
import boto3
import io
import datetime as dt
import numpy as np
from sqlalchemy import create_engine, MetaData
import time


def etl_inicial():

    # -- EXTRACT -- #
    # Creamos la conexión al bucket de minIO.
    s3 = boto3.resource('s3',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id='minio_admin',
                    aws_secret_access_key='minio_password'
    )

    # Se establece la conexión al servicio y se hace la lectura de archivos a la lista filenames
    filenames = [obj.key for obj in s3.Bucket('raw-data').objects.all()]
    month_stamp = filenames[4][30:37]
    raw_data = [name for name in filenames if name[: 13] == 'carga_inicial' and name[-1] != '/']

    # Se declaran 3 listas donde se van a almacenar los nombres de los archivos según su tipo (clima, zonas y viajes)
    # posteriormente se almacenan en estas listas los archivos o rutas para lectura
    weather = []
    zones = []
    trips = []

    for name in raw_data:
        if name[14:16] == 'NY':
            weather.append(name)
        elif name[14:18] == 'taxi':
            zones.append(name)
        elif name[14:20] == 'yellow':
            trips.append(name)
        else:
            print(f'{name} is an invalid filename.')

    # Levanta los dataframes correspondientes concatenando los archivos del bucket S3 según su tipo.
    weather_objs = [s3.Bucket('raw-data').Object(filename).get() for filename in weather]
    weather_dfs = [pd.read_csv(obj['Body'], parse_dates=['Datetime']) for obj in weather_objs]
    df_weather = pd.concat(weather_dfs, ignore_index=False) 

    zones_objs = [s3.Bucket('raw-data').Object(filename).get() for filename in zones]
    zones_dfs = [pd.read_csv(obj['Body']) for obj in zones_objs]
    df_zones = pd.concat(zones_dfs, ignore_index=False)

    trips_objs = [s3.Bucket('raw-data').Object(filename).get() for filename in trips]
    trips_dfs = [pd.read_parquet(io.BytesIO(obj['Body'].read())) for obj in trips_objs]
    df1 = pd.concat(trips_dfs, ignore_index=False)

    # -- CLEANING -- #
    # - Reindexamos el df agregando la estampa del mes.
    stamp_id = int(month_stamp.replace('-', '') + '00000000')
    df1.index += stamp_id
    # - VendorId sólo acepta 1 o 2, si se da algo fuera de esto se imputa por la moda.
    supported_values = [1, 2]  # creamos una variable con los valores admitidos
    mode_vendor = int(df1['VendorID'].mode())   # la moda de la columna
    # implementamos un bucle for para ir comprobando que cada valor esté dentro de los valores, caso contrario -
    # se imputa por la moda
    for i in df1['VendorID']:
        if i not in supported_values:
            df1['VendorID'].replace(to_replace=i, value=mode_vendor, inplace=True)

    # - tpep_pickup_datetime acepta registros entre 01-01-2018 y 31-01-2018 (23:59:59 hs), fuera de esto/NaT se imputa
    # por el último valor dentro del rango. *Decisión final: Fuera del rango se desestima
    date = df1.tpep_pickup_datetime.mode()[0]
    year = date.year
    month = date.month
    min_date = dt.datetime(year, month, 1)  # establecemos la fecha minima
    if month == 12:
        year += 1
        month = 1
    else:
        month += 1
    max_date = dt.datetime(year, month, 1)  # establecemos la fecha maxima
    df1.query("tpep_pickup_datetime <= @max_date", inplace=True)  # filtramos
    df1.query("tpep_pickup_datetime >= @min_date", inplace=True)

    # - tpep_dropoff_datetime Acepta valores entre 01-01-2018 y 01-02-2018 (hasta el mediodía 12pm).
    # Si se pasa del rango, imputar con el último valor dentro del rango.
    min_date1 = min_date  # establecemos la fecha minima
    max_date1 = dt.datetime(year, month, 1, 12)  # establecemos la fecha maxima (hasta el mediodia)

    # - tpep_dropoff_datetime se desestima, se reemplaza por el campo 'Duración del viaje'.
    # Primero creamos la columna 'Travel_time'
    df1['Travel_time'] = (df1['tpep_dropoff_datetime'] - df1['tpep_pickup_datetime']).dt.seconds/60
    # Ahora procedemos a eliminar la columna 'tpep_dropoff_datetime'
    df1 = df1.drop(['tpep_dropoff_datetime'], axis=1)

    # - Passanger_count acepta registros entre 1-4, fuera de esto se imputa por moda. *Decisión final: Imputar 0 por 1.
    supported_values = [*range(1, 5)]  # creamos una variable con los valores admitidos
    mode_passenger = int(df1['passenger_count'].mode())  # la moda de la columna
    # implementamos un bucle for para ir comprobando que cada valor esté dentro de los valores, caso contrario -
    # se imputa por la moda (1).
    for i in df1['passenger_count']:
        if i not in supported_values:
            df1['passenger_count'].replace(to_replace=i, value=mode_passenger, inplace=True)

    # - Trip_distance 3 sigmas para detectar outliers, se desestiman los outliers superiores.
    # Los viajes con trip_distance = 0 se tratan de imputar usando el fare_amount + rate_code,
    # si siguen sin poder ser imputados se desestiman.
    below = df1['trip_distance'].mean() - 3 * df1['trip_distance'].std()
    above = df1['trip_distance'].mean() + 3 * df1['trip_distance'].std()

    df1.query("trip_distance < @above", inplace=True)  # filtramos los outliers superiores


    # - PU_Location y DO_location aceptan valores dentro del rango (1, 265)
    # Fuera de este rango se imputa por 265 (Unknown).
    def impute_value(p_x):
        """Imputa por 265 (Unknown) donde haya valores fuera de rango (1-265)."""
        if 1 <= p_x <= 265:
            return p_x
        return 265


    df1['PULocationID'] = df1['PULocationID'].apply(impute_value)
    df1['DOLocationID'] = df1['DOLocationID'].apply(impute_value)

    # - Rate_CodeID acepta valores dentro del rango (1-6), fuera de este rango se imputa por moda.
    mode = df1['RatecodeID'].mode()[0]


    def impute_mode(p_x):
        """Imputa la moda donde haya valores fuera de rango (1-6)"""
        if p_x not in [*range(1, 7)]:
            return mode
        return p_x


    df1['RatecodeID'] = df1['RatecodeID'].apply(impute_mode)

    # - Store_n_fwd actualmente no está en el ERD, drop a la columna a menos de que se nos ocurra algún uso.
    # *Decisión final: Drop
    df1 = df1.drop(columns=['store_and_fwd_flag'])


    # - Payment_type acepta valores (1-6), fuera de este rango se imputa por 5 (Unknown).
    def impute_type(p_x):
        """Imputa la moda donde haya valores fuera de rango (1-6)"""
        if p_x not in [*range(1, 7)]:
            return 5
        return p_x


    df1['payment_type'] = df1['payment_type'].apply(impute_type)

    # - Fare_Amount: Se agrega a la tabla de outliers, no se puede inferir los demás datos


    # - Extra acepta valores en (0.0, 0.5, 1.0) valores fuera del rango se imputan por moda.
    mode_extra = df1['extra'].mode()[0]


    def impute_extra(p_x):
        """Imputa la moda donde haya valores fuera de rango (0, 0.5, 1)"""
        if p_x not in [0, 0.5, 1]:
            return mode_extra
        return p_x


    df1['extra'] = df1['extra'].apply(impute_extra)


    # - MTA_tax Sólo acepta valores de 0 y 0.5.*Decisión final: si el viaje no es outlier imputar
    def impute_mta(p_x):
        """Imputa a 0.5 en MTA_tax donde haya valores distintos de este."""
        if p_x != 0.5:
            return 0.5
        return p_x


    df1['mta_tax'] = df1.loc[(~df1['payment_type'].isin([3, 4])), 'mta_tax'].apply(impute_mta)


    # - Improvement_surcharge Sólo acepta valores de (0.3), si el viaje no es outlier imputar.
    # *Decisión final: imputar en todos los casos a 0.3
    def impute_i_s(p_x):
        """Imputa el 0.3 en Improvement Surcharge donde haya valores distintos de este."""
        if p_x != 0.3:
            return 0.3
        return p_x


    df1['improvement_surcharge'] = df1.loc[(~df1['payment_type'].isin([3, 4])), 'improvement_surcharge'].apply(impute_i_s)

    # - Tip_amount acepta cualquier valor, considerar dropear o no . * Decisión final: Dropear
    df1 = df1.drop(['tip_amount'], axis=1)

    # Agregamos a un DataFrame de outliers los datos que no pueden ser tratados.
    df1['Outlier'] = 0

    df1.loc[(df1['mta_tax'].isna()), 'Outlier'] = 1
    df1.loc[(df1['trip_distance'] == 0), 'Outlier'] = 1
    df1.loc[(df1['fare_amount'] <= 0), 'Outlier'] = 1
    df1.loc[(df1['improvement_surcharge'].isna()), 'Outlier'] = 1
    df1.loc[((df1['PULocationID'] == 265) | (df1['DOLocationID'] == 265)), 'Outlier'] = 1

    df_outliers = df1.loc[df1['Outlier'] == 1]

    # -- SEGMENTACION -- #
    # -- Creamos una copia del DataFrame para no afectar el original --#
    df = df1.reset_index()
    df_outliers = df_outliers.reset_index()

    # -- Agregamos info del clima al DataFrame de viajes --#
    bors = df_zones.Borough.unique()
    df_bors = pd.DataFrame(enumerate(bors), columns=['IdBorough', 'Borough'])
    df_weather = df_weather.merge(right=df_bors, how='inner', on='Borough')
    df_weather['Key_1'] = df_weather.Datetime.dt.strftime('%Y%m%d%H')
    df_weather.IdBorough = [*map(str, df_weather.IdBorough)]
    df_weather['Key_final'] = df_weather.IdBorough + df_weather.Key_1


    df = df.merge(right=df_zones, how='left', left_on='PULocationID', right_on='LocationID')
    df = df.merge(right=df_bors, how='inner', on='Borough')
    df['Key_1'] = df['tpep_pickup_datetime'].dt.strftime('%Y%m%d%H')
    df.IdBorough = [*map(str, df.IdBorough)]
    df['Key_final'] = df.IdBorough + df.Key_1
    df = df.merge(right=df_weather, how='left', on='Key_final')
    columns = list(df1) + ['index', 'Temperature', 'Precip_Type']
    df = df[columns]

    # -- Creación de tablas --#
    # -- Vendor --#
    df_vendor = pd.DataFrame({
        'VendorID': [1, 2],
        'Vendor': ['Creative Mobile Technologies, LLC', 'VeriFone Inc.']
    })

    # -- Borough --#
    boroughs = df_zones['Borough'].unique()

    df_borough = pd.DataFrame({
        'Borough': boroughs
    })
    df_borough.insert(loc=0, column='BoroughID', value=np.arange(1, len(df_borough)+1))

    # -- Service_Zone --#
    service_zones = df_zones['service_zone'].unique()

    df_service_zone = pd.DataFrame({
        'Service_Zone': service_zones
    })
    df_service_zone.insert(loc=0, column='Service_ZoneID', value=np.arange(1, len(df_service_zone)+1))
    df_service_zone.loc[df_service_zone['Service_Zone'].isna(), 'Service_Zone'] = 'Unknown'

    # -- Zone --#
    df_zone = df_zones[['LocationID', 'Zone', 'Borough', 'service_zone']]
    df_zone.loc[df_zone.LocationID == 265, 'Zone'] = 'Unknown'
    df_zone.rename(columns={
        'LocationID': 'ZoneID'
    }, inplace=True)


    def replace_bor_for_index(p_x):
        """
        Retorna el índice correspondiente al Borough.
        :type p_x: str
        :param p_x: Borough del cuál deseo saber el índice.
        """
        for j, bor in enumerate(df_borough['Borough']):
            if bor == p_x:
                return df_borough.iloc[j]['BoroughID']


    def replace_zone_for_index(p_x):
        """
        Retorna el índice correspondiente a la Service Zone.
        :type p_x: str
        :param p_x:Service Zone de la cuál deseo saber el índice.
        """
        for j, zone in enumerate(df_service_zone['Service_Zone']):
            if zone == p_x:
                return df_service_zone.iloc[j]['Service_ZoneID']


    df_zone['BoroughID'] = df_zone['Borough'].apply(replace_bor_for_index)
    df_zone['Service_ZoneID'] = df_zone['service_zone'].apply(replace_zone_for_index)


    # -- Rate_Code --#
    df_rate_code = pd.DataFrame({
        'RateCodeID': [*range(1, 7)],
        'RateCode': ['Standard rate', 'JFK', 'Newark', 'Nassau or Westchester', 'Negotiated fare', 'Group ride']
    })

    # -- Payment_Type --#
    df_payment_type = pd.DataFrame({
        'PaymentTypeID': [*range(1, 7)],
        'PaymentType': ['Credit card', 'Cash', 'No charge', 'Dispute', 'Unknown', 'Voided trip']
    })

    # -- Precip_Type --#
    df_precip_type = pd.DataFrame({
        'Precip_Type': ['No precipitacion', 'Rain', 'Snow', 'Rain and Snow']
    })
    df_precip_type.insert(loc=0, column='Precip_TypeID', value=np.arange(len(df_precip_type)))

    # -- Calendar --#
    df['tpep_pickup_datetime'] = [*map(str, df['tpep_pickup_datetime'])]
    only_dates = df['tpep_pickup_datetime'].str.split()

    for i, elem in enumerate(only_dates):
        only_dates[i] = elem[0]

    unique_dates = list(only_dates.unique())

    df_calendar = pd.DataFrame({
        'Date': unique_dates
    })
    df_calendar['Date'] = pd.to_datetime(df_calendar['Date'])
    df_calendar.insert(loc=0, column='DateID', value=df_calendar.Date.dt.strftime('%Y%m%d'))

    df_calendar['Year'] = df_calendar['Date'].dt.year
    df_calendar['Month'] = df_calendar['Date'].dt.month
    df_calendar['Day'] = df_calendar['Date'].dt.day
    df_calendar['Week'] = df_calendar['Date'].dt.isocalendar().week
    df_calendar['Day_of_Week'] = df_calendar['Date'].dt.day_name()

    # -- Trip --#
    datetime = df['tpep_pickup_datetime'].str.split()
    dates = []
    times = []
    for date, time in datetime:
        dates.append(date)
        times.append(time)

    df_trip = pd.DataFrame({
        'IdTrip': df['index'],
        'VendorID': df['VendorID'],
        'Date': dates,
        'PU_Time': times,
        'Duration': df['Travel_time'],
        'Passenger_count': df['passenger_count'],
        'Distance': df['trip_distance'],
        'PU_IdZone': df['PULocationID'],
        'DO_IdZone': df['DOLocationID'],
        'Temperature': df['Temperature'],
        'Precip_Type': df['Precip_Type']
    })

    df_trip.Date = pd.to_datetime(df_trip.Date)
    df_trip.Date = df_trip.Date.dt.strftime('%Y%m%d')
    df_trip.Duration = df_trip.Duration.round()
    df_trip.Precip_Type = df_trip.Precip_Type.round()

    # -- Payment --#
    df_payment = df[['RatecodeID', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
                    'improvement_surcharge', 'tolls_amount', 'total_amount']]

    df_payment.insert(loc=0, column='IdTrip', value=df_trip['IdTrip'])
    df_payment.insert(loc=0, column='IdPayment', value=df_trip['IdTrip'])

    # -- Outlier -- #
    df_outliers = df_outliers[['index']]

    # -- Eliminamos columnas previamente utilizadas, pero que no cumplen con el modelo entidad-relación planteado --#
    df_zone = df_zone.drop(columns=['Borough', 'service_zone'])

    # -- Renombrar Columnas - Normalizar --#
    df_vendor.columns = ['idvendor', 'vendor']

    df_calendar.columns = ['iddate', 'date', 'year', 'month', 'day', 'week', 'day_of_week']

    df_borough.columns = ['idborough', 'borough']

    df_service_zone.columns = ['idservice_zone', 'service_zone']

    df_trip.columns = ['idtrip', 'idvendor', 'iddate', 'pu_time', 'duration', 'passenger_count',
                    'distance', 'pu_idzone', 'do_idzone', 'temperature', 'idprecip_type']

    df_precip_type.columns = ['idprecip_type', 'precip_type']

    df_rate_code.columns = ['idrate_code', 'rate_code']

    df_payment_type.columns = ['idpayment_type', 'payment_type']

    df_zone.columns = ['idzone', 'zone', 'idborough', 'idservice_zone']

    df_payment.columns = ['idpayment', 'idtrip', 'idrate_code', 'idpayment_type', 'fare_amount',
                        'extra', 'mta_tax', 'improvement_surcharge', 'tolls_amount', 'total_amount']

    df_outliers.columns = ['idrecord']

    # -- CARGA INICIAL -- #
    DBMS = 'postgresql'
    DRIVER = 'psycopg2'
    USER = 'postgres'
    PASSWORD = 'postgres'
    HOST = '172.22.2.13'
    PORT = '5432'
    DB_NAME = 'nyc_taxis'

    # crea la conexión a la base de datos, primero crea un engine que es basicamente una instancia de la base de datos
    # cuando ya está instanciada se crea la conexión a la base de datos
    # metadata es un objeto que contiene toda la información correspondiente a los features de la base de datos

    engine = create_engine(f'{DBMS}+{DRIVER}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
    connection = engine.connect()
    metadata = MetaData()

    # Carga los contenidos de los dataframes en las tablas correspondientes en la DB, se recomienda ejecutar uno por uno.
    # Para las tablas con mayor número de registros se recomienda trabajar con el parametro chunksize
    dataframes = [df_service_zone, df_borough, df_zone, df_vendor, df_calendar, df_precip_type, df_rate_code,
                df_payment_type, df_trip, df_payment, df_outliers]
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
            df_trip.loc[(df_trip['idprecip_type'].isna()), 'idprecip_type'] = 0
            df_trip.idprecip_type = [*map(round, df_trip.idprecip_type)]
            df_trip.duration = [*map(int, df_trip.duration)]
            sql_copy_opt(df_trip, name, engine)
        elif name == 'payment':
            sql_copy_opt(df_payment, name, engine)
        else:      
            fill_table(name, dataframes[i])

    # Cerramos las conexiones
    connection.close()
    engine.dispose()
    