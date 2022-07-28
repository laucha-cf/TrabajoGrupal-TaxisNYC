import io

import pandas as pd
from sqlalchemy import create_engine
import time


def load(to_ingest, dbms, driver, user, passw, host, port, db_name):
    # crea la conexión a la base de datos, primero crea un engine que es basicamente una instancia de la base de datos
    # cuando ya está instanciada se crea la conexión a la base de datos
    # metadata es un objeto que contiene toda la información correspondiente a los features de la base de datos

    engine = create_engine(f'{dbms}+{driver}://{user}:{passw}@{host}:{port}/{db_name}')
    connection = engine.connect()

    # Carga los contenidos de los dataframes en las tablas correspondientes en la DB, se recomienda ejecutar uno por uno.
    # Para las tablas con mayor número de registros se recomienda trabajar con el parametro chunksize
    dataframes = to_ingest
    tables_names = ['calendar', 'trip', 'payment', 'aux_outlier']
    df_times = pd.DataFrame({'tables': tables_names})
    start_tms = []
    end_tms = []


    def fill_table(p_name, p_dataframe):
        """
        Función para poblar las tablas mediante SQLAlchemy.

        :type p_name: str
        :param p_name: Nombre de la tabla a poblar.

        :type p_dataframe: pandas.DataFrame
        :param p_dataframe: DataFrame a insertar en la tabla.
        """
        print(f'Insertando {p_name}...')
        start = time.time()
        p_dataframe.to_sql(p_name, engine, if_exists='append', index=False, method='multi')
        start_tms.append(start)
        end_tms.append(time.time())


    # Función para optimizar la carga de tablas grandes
    def sql_copy_opt(df, tablename, eng):
        """
        Optimiza el tiempo de carga para tablas grandes.

        :type df: pandas.DataFrame
        :param df: DataFrame a insertar en la tabla.

        :type tablename: str
        :param tablename: Nombre de la tabla a poblar.

        :type eng: sqlalchemy.engine.base.Engine
        :param eng: Engine usada para conectarse a la DB.
        """
        start = time.time()
        conn = eng.raw_connection()
        print(f'Insertando {tablename}...')
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cur.copy_from(output, tablename, null="")  # valores nulos se convierten a ''
        conn.commit()
        start_tms.append(start)
        end_tms.append(time.time())
        cur.close()


    # Poblamos todas las tablas
    for i, name in enumerate(tables_names):
        if name == 'trip':
            dataframes[i].loc[(dataframes[i]['idprecip_type'].isna()), 'idprecip_type'] = 0
            dataframes[i].idprecip_type = [*map(round, dataframes[i].idprecip_type)]
            dataframes[i].duration = [*map(round, dataframes[i].duration)]
            sql_copy_opt(dataframes[i], name, engine)
        elif name == 'payment':
            sql_copy_opt(dataframes[i], name, engine)
        else:      
            fill_table(name, dataframes[i])

    # Anotamos los tiempos de carga
    df_times['Start'] = start_tms
    df_times['Stop'] = end_tms
    df_times['RunTime'] = df_times['Stop'] - df_times['Start']
    df_times.to_csv('TiempoEjecucion.csv')

    # Cerramos las conexiones
    connection.close()
    engine.dispose()
