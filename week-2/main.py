import time
import datetime as dt
import pandas as pd

# -- Pipeline --#
if __name__ == '__main__':
    start = time.time()
    print('Extrayendo Data del Bucket (Extract.py)...')
    exec(open('Extract.py').read())
    
    print('Limpiando Datos (DataCleaning.py)...')
    exec(open('DataCleaning.py').read())
    
    # print('Moviendo datos ya procesados (s3move.py)...')
    # exec(open('s3move.py').read())
    
    print('Creación de la Base de Datos (ddl1_tables.py)...')
    exec(open('ddl1_tables.py').read())
    
    print('Segmentación de Tablas y Normalización de Campos (Segmentacion_Tablas.py)...')
    exec(open('Segmentacion_Tablas.py').read())
    
    print('Cargamos la data a la Base de Datos (Carga_inicial.py)...')
    exec(open('Carga_inicial.py').read())
    stop = time.time()
    df = pd.DataFrame({
        'Fecha': [dt.date.today()],
        'Tiempo_Mins': [(stop-start)/60]
    })
    df.to_csv('TiempoGlobal.csv', index=False)
