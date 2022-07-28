from Extract_incremental import extract
from cleaning_incremental import cleaning
from segmentacion_incremental import transform
from carga_incremental import load
from s3move import move

# -- Variables globales -- #
DBMS = 'postgresql'
DRIVER = 'psycopg2'
USER = 'postgres'
PASSWORD = 'postgres'
HOST = '172.24.230.154'
PORT = '5432'
DB_NAME = 'G9'


# -- Pipeline --#
if __name__ == '__main__':
    df_weather, df_zones, df_trips, stamp = extract()
    df_trip_clean, df_outlier = cleaning(df_trips, stamp)
    tables = transform(df_trip_clean, df_outlier, df_zones, df_weather)
    load(tables, DBMS, DRIVER, USER, PASSWORD, HOST, PORT, DB_NAME)
    # move()
