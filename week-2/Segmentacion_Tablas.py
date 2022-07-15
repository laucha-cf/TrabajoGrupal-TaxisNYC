import pandas as pd
import numpy as np
import datetime as dt

#-- Cargamos los datos de las zonas dentro de Nueva York --#
df_all_zones = pd.read_csv('../data/taxi+_zone_lookup.csv')
#-- Cargamos los datos de los viajes en taxi. Datos previamente procesados --#
df_all = pd.read_csv('../processed_data/data_taxis_nyc_2018.csv', parse_dates=['tpep_pickup_datetime'])
#-- Eliminamos las columnas innecesarias --#
df_all = df_all.drop(columns=['Unnamed: 0'])
#-- Creamos una copia del DataFrame para no afectar el original --#
df = df_all.copy()

#-- Cargamos los datos del clima para cada Borough --#
df_weather = pd.read_csv('../data/NY_Boroughs_Weathers_01-2018.csv', parse_dates=['Datetime'])

#-- Agregamos info del clima al DataFrame de viajes --#
bors = df_all_zones.Borough.unique()
df_bors = pd.DataFrame(enumerate(bors), columns=['IdBorough', 'Borough'])
df_weather = df_weather.merge(right=df_bors, how='inner', on='Borough')
df_weather['Key_1'] = df_weather.Datetime.dt.strftime('%Y%m%d%H')
df_weather.IdBorough = [*map(str, df_weather.IdBorough)]
df_weather['Key_final'] = df_weather.IdBorough + df_weather.Key_1

# %%
df = df.merge(right=df_all_zones, how='left', left_on='PULocationID', right_on='LocationID')
df = df.merge(right=df_bors, how='inner', on='Borough')
df['Key_1'] = df['tpep_pickup_datetime'].dt.strftime('%Y%m%d%H')
df.IdBorough = [*map(str, df.IdBorough)]
df['Key_final'] = df.IdBorough + df.Key_1
df = df.merge(right=df_weather, how='left', on='Key_final')
columns = list(df_all) + ['Temperature', 'Precip_Type']
df = df[columns]


#-- Creación de tablas --#
#-- Vendor --#
df_vendor = pd.DataFrame({
    'VendorID': [1,2],
    'Vendor': ['Creative Mobile Technologies, LLC', 'VeriFone Inc.']
})

#-- Borough --#
boroughs = df_all_zones['Borough'].unique()

df_borough = pd.DataFrame({
    'Borough': boroughs
})
df_borough.insert(loc=0, column='BoroughID', value=range(1, len(df_borough)+1))


#-- Service_Zone --#
service_zones = df_all_zones['service_zone'].unique()

df_service_zone = pd.DataFrame({
    'Service_Zone': service_zones
})
df_service_zone.insert(loc=0, column='Service_ZoneID', value=range(1, len(df_service_zone)+1))
df_service_zone.loc[df_service_zone['Service_Zone'].isna(), 'Service_Zone'] = 'Unknown'

#-- Zone --#
df_zone = df_all_zones[['LocationID', 'Zone', 'Borough', 'service_zone']]
df_zone.loc[df_zone.LocationID==265, 'Zone'] = 'Unknown'
df_zone.rename(columns={
    'LocationID': 'ZoneID'
}, inplace=True)

def replaceForIndex( p_x ):
    for i, elem in enumerate(df_borough['Borough']):
        if elem == p_x:
            return df_borough.iloc[i]['BoroughID']

df_zone['BoroughID'] = df_zone['Borough'].apply(replaceForIndex)

def replaceForIndex( p_x ):
    for i, elem in enumerate(df_service_zone['Service_Zone']):
        if elem == p_x:
            return df_service_zone.iloc[i]['Service_ZoneID']

df_zone['Service_ZoneID'] = df_zone['service_zone'].apply(replaceForIndex)

#-- Rate_Code --#
df_rate_code = pd.DataFrame({
    'RateCodeID': [1,2,3,4,5,6],
    'RateCode':['Standard rate', 'JFK', 'Newark', 'Nassau or Westchester', 'Negotiated fare', 'Group ride']
})

#-- Payment_Type --#
df_payment_type = pd.DataFrame({
    'PaymentTypeID': [1,2,3,4,5,6],
    'PaymentType':['Credit card', 'Cash', 'No charge', 'Dispute', 'Unknown', 'Voided trip']
})

#-- Precip_Type --#
df_precip_type = pd.DataFrame({
    'Precip_Type': ['No precipitacion', 'Rain', 'Snow', 'Rain and Snow']
})
df_precip_type.insert(loc=0, column='Precip_TypeID', value=range(len(df_precip_type)))

#-- Calendar --#
df['tpep_pickup_datetime'] = [*map(str, df['tpep_pickup_datetime'])]
only_dates = df['tpep_pickup_datetime'].str.split()

for i, elem in enumerate(only_dates):
    only_dates[i] = elem[0]
    
unique_dates = list(only_dates.unique())

df_calendar = pd.DataFrame({
    'Date': unique_dates
})
df_calendar.insert(loc=0, column='DateID', value=range(1, len(df_calendar)+1))

df_calendar['Date'] = pd.to_datetime(df_calendar['Date'])
df_calendar['Year']     = df_calendar['Date'].dt.year
df_calendar['Month']    = df_calendar['Date'].dt.month
df_calendar['Day']      = df_calendar['Date'].dt.day
df_calendar['Week']     = df_calendar['Date'].dt.week
df_calendar['Day_of_Week'] = df_calendar['Date'].dt.day_of_week

#-- Trip --#
datetime = df['tpep_pickup_datetime'].str.split()
dates = []
times = []
for date, time in datetime:
    dates.append(date)
    times.append(time)

df_trip = pd.DataFrame({
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

df_trip.insert(loc=0, column='IdTrip', value=range(len(df_trip)))

def addBorough( p_x ):
    for i, elem in enumerate(df_zone['ZoneID']):
        if elem == p_x:
            return df_zone.iloc[i]['Borough']

#-- Payment --#
df_payment = df[['RatecodeID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'improvement_surcharge', 'tolls_amount', 'total_amount']]

df_payment.insert(loc=0, column='IdTrip', value=df_trip['IdTrip'])

df_payment.insert(loc=0, column='IdPayment', value=range(len(df_payment)))


#-- Eliminamos columnas previamente utilizadas, pero que no cumplen con el modelo entidad-relación planteado --#
df_zone = df_zone.drop(columns=['Borough', 'service_zone'])

#-- Renombrar Columnas - Normalizar --#
df_vendor.columns = ['IdVendor', 'Vendor']
df_calendar.rename(columns={'DateID':'IdDate'}, inplace=True)
df_borough.columns = ['IdBorough', 'Borough']
df_service_zone.rename(columns={'Service_ZoneID':'IdService_Zone'}, inplace=True)
df_trip.rename(columns={'VendorID': 'IdVendor'}, inplace=True)
df_precip_type.rename(columns={'Precip_TypeID': 'IdPrecip_Type'}, inplace=True)
df_rate_code.rename(columns={'RateCodeID': 'IdRate_Code', 'RateCode': 'Rate_Code'}, inplace=True)
df_payment_type.rename(columns={'PaymentTypeID': 'IdPayment_Type','PaymentType': 'Payment_Type'}, inplace=True)
df_zone.rename(columns={'ZoneID': 'IdZone','BoroughID': 'IdBorough','Service_ZoneID': 'IdService_Zone'},inplace=True)
df_payment.rename(columns={'RatecodeID': 'IdRate_Code','payment_type': 'IdPayment_Type','fare_amount': 'Fare_Amount','extra': 'Extra','mta_tax': 'MTA_Tax','improvement_surcharge': 'Improvement_Surcharge','tolls_amount': 'Tolls_Amount','total_amount': 'Total_Amount'}, inplace=True)

#-- Exportar a archivos CSV --#
df_vendor.to_csv('../tables/vendor.csv')
df_calendar.to_csv('../tables/calendar.csv')
df_borough.to_csv('../tables/borough.csv')
df_service_zone.to_csv('../tables/service_zone.csv')
df_trip.to_csv('../tables/trip.csv')
df_precip_type.to_csv('../tables/precip_type.csv')
df_rate_code.to_csv('../tables/rate_code.csv')
df_payment_type.to_csv('../tables/payment_type.csv')
df_zone.to_csv('../tables/zone.csv')
df_payment.to_csv('../tables/payment.csv')


