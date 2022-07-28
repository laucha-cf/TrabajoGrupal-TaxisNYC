from Extract import extract
from DataCleaning import cleaning
from ddl1_tables import ddl
from Segmentacion_Tablas import transform
from Carga_inicial import load
from s3move import move

# -- Variables globales -- #
DBMS = 'postgresql'
DRIVER = 'psycopg2'
USER = 'postgres'
PASSWORD = 'postgres'
HOST = 'localhost'
PORT = '5432'
DB_NAME = 'G9'


# -- Pipeline --#
if __name__ == '__main__':
    df_weather, df_zones, df_trips, stamp = extract()
    df_trip_clean, df_outlier = cleaning(df_trips, stamp)
    ddl(DBMS, USER, PASSWORD, HOST, PORT, DB_NAME)
    tables = transform(df_trip_clean, df_outlier, df_zones, df_weather)
    load(tables, DBMS, DRIVER, USER, PASSWORD, HOST, PORT, DB_NAME)
    move()
    