import boto3

# Se instancia la sesión con las credenciales de acceso para la conexión.
sesion = boto3.session.Session(
    aws_access_key_id='AKIASPG3VE3KGTL2HGMN',
    aws_secret_access_key='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',
    region_name='us-east-1'
)

# Se establece la conexión al servicio
connection = sesion.resource(service_name='s3')

# Se asignan a una colección los objetos con prefijo raw_data, es decir todos los archivos
# que se encuentran dentro de ese directorio, y el directorio como objeto, el directorio se ignora
# los archivos que se encuentran dentro se copian al directorio processed_data/raw_data y luego se
# borran del directorio raw_data original.

buck = connection.Bucket('henry-pg9')
files= buck.objects.filter(Prefix='raw_data/')
for file in files:
    if file.key[-1]=='/':
        continue
    else :
        copy_source = {
            'Bucket':'henry-pg9',
            'Key': file.key
        }
        moved = buck.Object('processed_data/'+file.key)
        moved.copy(copy_source)
        file.delete()