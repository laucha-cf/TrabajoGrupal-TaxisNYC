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

    # Extraemos la estampa del mes
    month_stamp = trips[0][30:37]

    # Levanta los dataframes correspondientes concatenando los archivos del bucket S3 según su tipo.
    weather_objs = [s3.Bucket('raw-data').Object(filename).get() for filename in weather]
    weather_dfs = [pd.read_csv(obj['Body'], parse_dates=['Datetime']) for obj in weather_objs]
    df_weather = pd.concat(weather_dfs, ignore_index=False) 

    zones_objs = [s3.Bucket('raw-data').Object(filename).get() for filename in zones]
    zones_dfs = [pd.read_csv(obj['Body']) for obj in zones_objs]
    df_zones = pd.concat(zones_dfs, ignore_index=False)

    trips_objs = [s3.Bucket('raw-data').Object(filename).get() for filename in trips]
    trips_dfs = [pd.read_parquet(io.BytesIO(obj['Body'].read())) for obj in trips_objs]
    df_trips = pd.concat(trips_dfs, ignore_index=False)

    return df_weather, df_zones, df_trips, month_stamp
