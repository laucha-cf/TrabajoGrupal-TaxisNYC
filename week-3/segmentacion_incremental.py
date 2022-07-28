import pandas as pd


def transform(df1, df_outliers, df_zones, df_weather):
    # -- Creamos una copia del DataFrame para no afectar el original --#
    df = df1.reset_index()
    df_outliers = df_outliers.reset_index()

    # -- Agregamos info del clima al DataFrame de viajes --#
    bors = df_zones.Borough.unique()
    df_bors = pd.DataFrame(enumerate(bors), columns=['IdBorough', 'Borough'])
    df_weather = df_weather.merge(right=df_bors, how='inner', on='Borough')
    df_weather['Key_1'] = df_weather.Datetime.dt.strftime('%Y%m%d%H')
    df_weather.IdBorough = [*map(str, df_weather.IdBorough)]
    df_weather['Key_final'] = df_weather.IdBorough + df_weather.Key_1


    df = df.merge(right=df_zones, how='left', left_on='PULocationID', right_on='LocationID')
    df = df.merge(right=df_bors, how='inner', on='Borough')
    df['Key_1'] = df['tpep_pickup_datetime'].dt.strftime('%Y%m%d%H')
    df.IdBorough = [*map(str, df.IdBorough)]
    df['Key_final'] = df.IdBorough + df.Key_1
    df = df.merge(right=df_weather, how='left', on='Key_final')
    columns = list(df1) + ['index', 'Temperature', 'Precip_Type']
    df = df[columns]

    # -- Creación de tablas --#
    # -- Calendar --#
    df['tpep_pickup_datetime'] = [*map(str, df['tpep_pickup_datetime'])]
    only_dates = df['tpep_pickup_datetime'].str.split()

    for i, elem in enumerate(only_dates):
        only_dates[i] = elem[0]

    unique_dates = list(only_dates.unique())

    df_calendar = pd.DataFrame({
        'Date': unique_dates
    })
    df_calendar['Date'] = pd.to_datetime(df_calendar['Date'])
    df_calendar.insert(loc=0, column='DateID', value=df_calendar.Date.dt.strftime('%Y%m%d'))

    df_calendar['Year'] = df_calendar['Date'].dt.year
    df_calendar['Month'] = df_calendar['Date'].dt.month
    df_calendar['Day'] = df_calendar['Date'].dt.day
    df_calendar['Week'] = df_calendar['Date'].dt.isocalendar().week
    df_calendar['Day_of_Week'] = df_calendar['Date'].dt.day_name()

    # -- Trip --#
    datetime = df['tpep_pickup_datetime'].str.split()
    dates = []
    times = []
    for date, time in datetime:
        dates.append(date)
        times.append(time)

    df_trip = pd.DataFrame({
        'IdTrip': df['index'],
        'VendorID': df['VendorID'],
        'Date': dates,
        'PU_Time': times,
        'Duration': df['Travel_time'],
        'Passenger_count': df['passenger_count'],
        'Distance': df['trip_distance'],
        'PU_IdZone': df['PULocationID'],
        'DO_IdZone': df['DOLocationID'],
        'Temperature': df['Temperature'],
        'Precip_Type': df['Precip_Type']
    })

    df_trip.Date = pd.to_datetime(df_trip.Date)
    df_trip.Date = df_trip.Date.dt.strftime('%Y%m%d')
    df_trip.Duration = df_trip.Duration.round()
    df_trip.Precip_Type = df_trip.Precip_Type.round()

    # -- Payment --#
    df_payment = df[['RatecodeID', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
                    'improvement_surcharge', 'tolls_amount', 'total_amount']]

    df_payment.insert(loc=0, column='IdTrip', value=df_trip['IdTrip'])
    df_payment.insert(loc=0, column='IdPayment', value=df_trip['IdTrip'])

    # -- Outlier -- #
    df_outliers = df_outliers[['index']]

    # -- Renombrar Columnas - Normalizar --#

    df_calendar.columns = ['iddate', 'date', 'year', 'month', 'day', 'week', 'day_of_week']

    df_trip.columns = ['idtrip', 'idvendor', 'iddate', 'pu_time', 'duration', 'passenger_count',
                    'distance', 'pu_idzone', 'do_idzone', 'temperature', 'idprecip_type']

    df_payment.columns = ['idpayment', 'idtrip', 'idrate_code', 'idpayment_type', 'fare_amount',
                        'extra', 'mta_tax', 'improvement_surcharge', 'tolls_amount', 'total_amount']

    df_outliers.columns = ['idrecord']

    return [df_calendar, df_trip, df_payment, df_outliers]

