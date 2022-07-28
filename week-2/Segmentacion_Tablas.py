import pandas as pd
import numpy as np


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
    # -- Vendor --#
    df_vendor = pd.DataFrame({
        'VendorID': [1, 2],
        'Vendor': ['Creative Mobile Technologies, LLC', 'VeriFone Inc.']
    })

    # -- Borough --#
    boroughs = df_zones['Borough'].unique()

    df_borough = pd.DataFrame({
        'Borough': boroughs
    })
    df_borough.insert(loc=0, column='BoroughID', value=np.arange(1, len(df_borough)+1))

    # -- Service_Zone --#
    service_zones = df_zones['service_zone'].unique()

    df_service_zone = pd.DataFrame({
        'Service_Zone': service_zones
    })
    df_service_zone.insert(loc=0, column='Service_ZoneID', value=np.arange(1, len(df_service_zone)+1))
    df_service_zone.loc[df_service_zone['Service_Zone'].isna(), 'Service_Zone'] = 'Unknown'

    # -- Zone --#
    df_zone = df_zones[['LocationID', 'Zone', 'Borough', 'service_zone']]
    df_zone.loc[df_zone.LocationID == 265, 'Zone'] = 'Unknown'
    df_zone.rename(columns={
        'LocationID': 'ZoneID'
    }, inplace=True)


    def replace_bor_for_index(p_x):
        """
        Retorna el índice correspondiente al Borough.
        :type p_x: str
        :param p_x: Borough del cuál deseo saber el índice.
        """
        for j, bor in enumerate(df_borough['Borough']):
            if bor == p_x:
                return df_borough.iloc[j]['BoroughID']


    def replace_zone_for_index(p_x):
        """
        Retorna el índice correspondiente a la Service Zone.
        :type p_x: str
        :param p_x:Service Zone de la cuál deseo saber el índice.
        """
        for j, zone in enumerate(df_service_zone['Service_Zone']):
            if zone == p_x:
                return df_service_zone.iloc[j]['Service_ZoneID']


    df_zone['BoroughID'] = df_zone['Borough'].apply(replace_bor_for_index)
    df_zone['Service_ZoneID'] = df_zone['service_zone'].apply(replace_zone_for_index)


    # -- Rate_Code --#
    df_rate_code = pd.DataFrame({
        'RateCodeID': [*range(1, 7)],
        'RateCode': ['Standard rate', 'JFK', 'Newark', 'Nassau or Westchester', 'Negotiated fare', 'Group ride']
    })

    # -- Payment_Type --#
    df_payment_type = pd.DataFrame({
        'PaymentTypeID': [*range(1, 7)],
        'PaymentType': ['Credit card', 'Cash', 'No charge', 'Dispute', 'Unknown', 'Voided trip']
    })

    # -- Precip_Type --#
    df_precip_type = pd.DataFrame({
        'Precip_Type': ['No precipitacion', 'Rain', 'Snow', 'Rain and Snow']
    })
    df_precip_type.insert(loc=0, column='Precip_TypeID', value=np.arange(len(df_precip_type)))

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

    # -- Eliminamos columnas previamente utilizadas, pero que no cumplen con el modelo entidad-relación planteado --#
    df_zone = df_zone.drop(columns=['Borough', 'service_zone'])

    # -- Renombrar Columnas - Normalizar --#
    df_vendor.columns = ['idvendor', 'vendor']

    df_calendar.columns = ['iddate', 'date', 'year', 'month', 'day', 'week', 'day_of_week']

    df_borough.columns = ['idborough', 'borough']

    df_service_zone.columns = ['idservice_zone', 'service_zone']

    df_trip.columns = ['idtrip', 'idvendor', 'iddate', 'pu_time', 'duration', 'passenger_count',
                    'distance', 'pu_idzone', 'do_idzone', 'temperature', 'idprecip_type']

    df_precip_type.columns = ['idprecip_type', 'precip_type']

    df_rate_code.columns = ['idrate_code', 'rate_code']

    df_payment_type.columns = ['idpayment_type', 'payment_type']

    df_zone.columns = ['idzone', 'zone', 'idborough', 'idservice_zone']

    df_payment.columns = ['idpayment', 'idtrip', 'idrate_code', 'idpayment_type', 'fare_amount',
                        'extra', 'mta_tax', 'improvement_surcharge', 'tolls_amount', 'total_amount']

    df_outliers.columns = ['idrecord']

    return [df_service_zone, df_borough, df_zone, df_vendor, df_calendar, df_precip_type, df_rate_code,
            df_payment_type, df_trip, df_payment, df_outliers]
