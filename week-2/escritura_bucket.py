import boto3
from io import StringIO

# def write()
# # Se instancia la sesión con las credenciales de acceso para la conexión a S3 para carga de tablas al data lake.
# sesion = boto3.session.Session(
#     aws_access_key_id='AKIASPG3VE3KGTL2HGMN',
#     aws_secret_access_key='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',
#     region_name='us-east-1'
# )

# # Se establece la conexión al servicio
# connection = sesion.resource(service_name='s3')

# # Se declara una variable con el nombre del bucket
# bucket = 'raw-data'

# # Usando StringIO se crea un objeto en memoria que contiene una cadena de caracteres con la información de cada dataframe y se almacena en la variable csv_buffer
# # este bufer lo usamos para escribir al archivo csv y por últmo usando la conexión al servicio s3 guardamos en el bucket el dataframe como archivo csv.
# # Este método está limitado por la RAM, si el dataframe llega a ser más grande que la RAM disponible, el código entrará en una excepción y no almacenará nada.

# lista_dataframes = [df_vendor, df_calendar, df_borough, df_service_zone, df_trip, df_precip_type, df_rate_code, df_payment_type, df_zone, df_payment]
# lista_nombres = ['df_vendor', 'df_calendar', 'df_borough', 'df_service_zone', 'df_trip', 'df_precip_type', 'df_rate_code', 'df_payment_type', 'df_zone', 'df_payment']


# for i, df in enumerate(lista_dataframes):
#     print(f'Voy por {lista_nombres[i]}')
#     csv_buffer = StringIO()
#     df.to_csv(csv_buffer, index=False)
#     connection.Object(bucket,f'processed_data/tables/{lista_nombres[i]}.csv').put(Body=csv_buffer.getvalue())
