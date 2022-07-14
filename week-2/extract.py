# -- Librerías usada -- #
import pandas as pd

import boto3
import awswrangler as wr

# -- Conexión al servicio de almacenamiento -- #
conection = boto3.resource(service_name='s3')

# -- Lectura del bucket -- #
filenames = [obj.key for obj in conection.Bucket('henry-pg9').objects.all()]

# -- Clasificación de datos de la carpeta 'raw_data' -- #
weather = []
zones = []
trips = []

for name in filenames:
    if name[9:11] == 'NY':
        weather.append(name)
    elif name[9:13] == 'taxi':
        zones.append(name)
    elif name[9:15] == 'yellow':
        trips.append(name)
    else:
        print(f'{name} is an invalid filename.')

# -- Descarga de datasets y concatenación en un solo DataFrame por clase en caso de que sean varios archivos -- #
weather_objs = [conection.Bucket('henry-pg9').Object(filename).get() for filename in weather]
weather_dfs = [pd.read_csv(obj['Body']) for obj in weather_objs]
df_weather = pd.concat(weather_dfs, ignore_index=True) 

zones_objs = [conection.Bucket('henry-pg9').Object(filename).get() for filename in zones]
zones_dfs = [pd.read_csv(obj['Body'], index_col='LocationID') for obj in zones_objs]
df_zones = pd.concat(zones_dfs, ignore_index=True)

trips_dfs = [wr.s3.read_parquet(path=f's3://henry-pg9/{filename}') for filename in trips]
df_trips = pd.concat(trips_dfs, ignore_index=True)
