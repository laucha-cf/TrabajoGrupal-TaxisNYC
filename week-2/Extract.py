import pandas as pd
import boto3
import awswrangler as wr


sesion = boto3.session.Session(
    aws_access_key_id='AKIASPG3VE3KGTL2HGMN',
    aws_secret_access_key='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',
    region_name = 'us-east-1'
)


connection = sesion.resource(service_name='s3')
filenames = [obj.key for obj in connection.Bucket('henry-pg9').objects.all()]
weather = []
zones = []
trips = []

for name in filenames:
    if name[9:11] == 'NY':
        weather.append(name)
    elif name[9:13] == 'taxi':
        zones.append(name)
    elif name[9:15] == 'yellow':
        trips.append('s3://henry-pg9/'+name)
    else:
        print(f'{name} is an invalid filename.')

# -- Descarga de datasets y concatenación en un solo DataFrame por clase en caso de que sean varios archivos -- #
weather_objs = [connection.Bucket('henry-pg9').Object(filename).get() for filename in weather]
weather_dfs = [pd.read_csv(obj['Body']) for obj in weather_objs]
df_weather = pd.concat(weather_dfs, ignore_index=True) 

zones_objs = [connection.Bucket('henry-pg9').Object(filename).get() for filename in zones]
zones_dfs = [pd.read_csv(obj['Body'], index_col='LocationID') for obj in zones_objs]
df_zones = pd.concat(zones_dfs, ignore_index=True)

trips_dfs = wr.s3.read_parquet(trips, boto3_session=sesion, path_suffix='.parquet')