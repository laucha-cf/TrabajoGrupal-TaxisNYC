import pandas as pd
import datetime as dt

# Cargamos los datos
df = pd.read_parquet('../data/yellow_tripdata_2018-01.parquet')

# Hacemos una copia para trabajar con los datos y para evitar que en un error afecte los datos originales
df1 = df.copy()  

# - VendorId sólo acepta 1 o 2, si se da algo fuera de esto se imputa por la moda.
supported_values = [1, 2]  # creamos una variable con los valores admitidos
mode_vendor = int(df1['VendorID'].mode())   # la moda de la columna
# implementamos un bucle for para ir comprobando que cada valor esté dentro de los valores, caso contrario -
# se imputa por la moda
for i in df1['VendorID']:
    if i not in supported_values:
        df1['VendorID'].replace(to_replace=i, value=mode_vendor, inplace=True)

# - tpep_pickup_datetime acepta registros entre 01-01-2018 y 31-01-2018 (23:59:59 hs), fuera de esto/NaT se imputa
# por el último valor dentro del rango. *Decisión final: Fuera del rango se desestima
min_date = dt.datetime(2018, 1, 1)  # establecemos la fecha minima
max_date = dt.datetime(2018, 2, 1)  # establecemos la fecha maxima
df1.query("tpep_pickup_datetime <= @max_date", inplace=True)  # filtramos
df1.query("tpep_pickup_datetime >= @min_date", inplace=True)

# - tpep_dropoff_datetime Acepta valores entre 01-01-2018 y 01-02-2018 (hasta el mediodía 12pm).
# Si se pasa del rango, imputar con el último valor dentro del rango.
min_date1 = dt.datetime(2018, 1, 1)  # establecemos la fecha minima
max_date1 = dt.datetime(2018, 2, 1, 12)  # establecemos la fecha maxima (hasta el mediodia)

# - tpep_dropoff_datetime se desestima, se reemplaza por el campo 'Duración del viaje'.
# Primero creamos la columna 'Travel_time'
df1['Travel_time'] = (df1['tpep_dropoff_datetime'] - df1['tpep_pickup_datetime']).dt.seconds/60
# Ahora procedemos a eliminar la columna 'tpep_dropoff_datetime'
df1 = df1.drop(['tpep_dropoff_datetime'], axis=1)

# - Passanger_count acepta registros entre 1-4, fuera de esto se imputa por moda. *Decisión final: Imputar 0 por 1.
supported_values = [*range(1, 5)]  # creamos una variable con los valores admitidos
mode_passenger = int(df1['passenger_count'].mode())  # la moda de la columna
# implementamos un bucle for para ir comprobando que cada valor esté dentro de los valores, caso contrario -
# se imputa por la moda (1).
for i in df1['passenger_count']:
    if i not in supported_values:
        df1['passenger_count'].replace(to_replace=i, value=mode_passenger, inplace=True)

# - Trip_distance 3 sigmas para detectar outliers, se desestiman los outliers superiores.
# Los viajes con trip_distance = 0 se tratan de imputar usando el fare_amount + rate_code,
# si siguen sin poder ser imputados se desestiman.
below = df1['trip_distance'].mean() - 3 * df1['trip_distance'].std()
above = df1['trip_distance'].mean() + 3 * df1['trip_distance'].std()

df1.query("trip_distance < @above", inplace=True)  # filtramos los outliers superiores

# - PU_Location y DO_location aceptan valores dentro del rango (1, 265)
# Fuera de este rango se imputa por 265 (Unknown).


def impute_value(p_x):
    '''Imputa 265 (Unknown) donde haya valores fuera de rango (1-265)
    param: p_x int'''
    if 1 <= p_x <= 265:
        return p_x
    return 265


df1['PULocationID'] = df1['PULocationID'].apply(impute_value)
df1['DOLocationID'] = df1['DOLocationID'].apply(impute_value)

# - Rate_CodeID acepta valores dentro del rango (1-6), fuera de este rango se imputa por moda.
mode = df1['RatecodeID'].mode()[0]


def impute_mode(p_x):
    '''Imputa la moda donde haya valores fuera de rango (1-6)
    param: p_x int
    '''
    if p_x not in [*range(1, 7)]:
        return mode
    return p_x


df1['RatecodeID'] = df1['RatecodeID'].apply(impute_mode)

# - Store_n_fwd actualmente no está en el ERD, drop a la columna a menos de que se nos ocurra algún uso.
# *Decisión final: Drop
df1 = df1.drop(columns=['store_and_fwd_flag'])

# - Payment_type acepta valores (1-6), fuera de este rango se imputa por 5 (Unknown).


def impute_type(p_x):
    '''Imputa la moda donde haya valores fuera de rango (1-6)
    param: p_x int
    '''
    if p_x not in [*range(1, 7)]:
        return 5
    return p_x


df1['payment_type'] = df1['payment_type'].apply(impute_type)

# - Fare_Amount: Se agrega a la tabla de outliers, no se puede inferir los demás datos


# - Extra acepta valores en (0.0, 0.5, 1.0) valores fuera del rango se imputan por moda.
mode_extra = df1['extra'].mode()[0]


def impute_extra(p_x):
    '''Imputa la moda donde haya valores fuera de rango (0, 0.5, 1)
    param: p_x int
    '''
    if p_x not in [0, 0.5, 1]:
        return mode_extra
    return p_x


df1['extra'] = df1['extra'].apply(impute_extra)

# - MTA_tax Sólo acepta valores de 0 y 0.5.*Decisión final: si el viaje no es outlier imputar
def impute_mta(p_x):
    '''Imputa el 0.5 en MTA_tax donde haya valores distintos de este
    param: p_x Float
    '''
    if p_x != 0.5:
        return 0.5
    return p_x


df1['mta_tax'] = df1.loc[(~df1['payment_type'].isin([3, 4])), 'mta_tax'].apply(impute_mta)

# - Improvement_surcharge Sólo acepta valores de (0.3), si el viaje no es outlier imputar.
# *Decisión final: imputar en todos los casos a 0.3


def impute_i_s(p_x):
    '''Imputa el 0.3 en Improvement Surcharge donde haya valores distintos de este
    param: p_x Float
    '''
    if p_x != 0.3:
        return 0.3
    return p_x


df1['improvement_surcharge'] = df1.loc[(~df1['payment_type'].isin([3, 4])), 'improvement_surcharge'].apply(impute_i_s)

# - Tip_amount acepta cualquier valor, considerar dropear o no . * Decisión final: Dropear
df1 = df1.drop(['tip_amount'], axis=1)

# Agregamos a un DataFrame de outliers los datos que no pueden ser tratados.
df1['Outlier'] = 0

df1.loc[(df1['mta_tax'].isna()), 'Outlier'] = 1
df1.loc[(df1['trip_distance'] == 0), 'Outlier'] = 1
df1.loc[(df1['fare_amount'] <= 0), 'Outlier'] = 1
df1.loc[(df1['improvement_surcharge'].isna()), 'Outlier'] = 1
df1.loc[((df1['PULocationID'] == 265) & (df1['DOLocationID'] == 265)), 'Outlier'] = 1

df_outliers = df1.loc[df1['Outlier'] == 1]

df_outliers.to_csv('../processed_data/outliers.csv')
df1.to_csv('../processed_data/data_taxis_nyc_2018.csv', index=False)
