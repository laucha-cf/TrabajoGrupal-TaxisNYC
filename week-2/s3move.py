import boto3

def move():
    # Se establece la conexión al servicio
    connection = boto3.resource(service_name='s3',
                        endpoint_url='http://localhost:9000',
                        aws_access_key_id='minio_admin',
                        aws_secret_access_key='minio_password'
    )

    # Se asignan a una colección los objetos con prefijo raw_data, es decir todos los archivos
    # que se encuentran dentro de ese directorio, y el directorio como objeto, el directorio se ignora
    # los archivos que se encuentran dentro se copian al directorio processed_data/raw_data y luego se
    # borran del directorio raw_data original.

    buck = connection.Bucket('raw-data')
    files= buck.objects.filter(Prefix='carga_inicial/')
    for file in files:
        if file.key[-1]=='/':
            continue
        else :
            copy_source = {
                'Bucket':'raw-data',
                'Key': file.key
            }
            moved = buck.Object('ingested_data/' + file.key)
            moved.copy(copy_source)
            file.delete()
