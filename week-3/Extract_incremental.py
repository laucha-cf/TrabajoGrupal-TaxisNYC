# SCRIPT DE CONEXIÓN A AMAZON S3 PARA DESCARGA DE DATOS DESDE EL DATA LAKE
import io

import pandas as pd
import boto3


def extract():
    # Se instancia la sesión con las credenciales de acceso para la conexión.
    s3 = boto3.resource(service_name='s3',
                        endpoint_url='http://localhost:9000',
                        aws_access_key_id='minio_admin',
                        aws_secret_access_key='minio_password'
    )

    # Se establece la conexión al servicio y se hace la lectura de archivos a la lista filenames
    filenames = [obj.key for obj in s3.Bucket('raw-data').objects.all()]
    raw_data = [name for name in filenames if name[: 17] == 'carga_incremental' and name[-1] != '/']

    # Se declaran 3 listas donde se van a almacenar los nombres de los archivos según su tipo (clima, zonas y viajes)
    # posteriormente se almacenan en estas listas los archivos o rutas para lectura
    weather = []
    trips = []

    for name in raw_data:
        if name[18:20] == 'NY':
            weather.append(name)
        elif name[18:24] == 'yellow':
            trips.append(name)
        else:
            print(f'{name} is an invalid filename.')

    # Extraemos la estampa del mes
    month_stamp = trips[0][34:41]

    # Levanta los dataframes correspondientes concatenando los archivos del bucket S3 según su tipo.
    weather_objs = [s3.Bucket('raw-data').Object(filename).get() for filename in weather]
    weather_dfs = [pd.read_csv(obj['Body'], parse_dates=['Datetime']) for obj in weather_objs]
    df_weather = pd.concat(weather_dfs, ignore_index=False) 

    zones_obj = s3.Bucket('raw-data').Object('ingested_data/carga_inicial/taxi+_zone_lookup.csv').get()
    df_zones = pd.read_csv(zones_obj['Body'])

    trips_objs = [s3.Bucket('raw-data').Object(filename).get() for filename in trips]
    trips_dfs = [pd.read_parquet(io.BytesIO(obj['Body'].read())) for obj in trips_objs]
    df_trips = pd.concat(trips_dfs, ignore_index=False)

    return df_weather, df_zones, df_trips, month_stamp
