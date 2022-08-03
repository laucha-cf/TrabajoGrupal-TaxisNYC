import requests  # Lo utilizamos para pedir solicitudes a nuestra API


#API KEY
#api_file = open('week-3\API-KEY-googlemaps.txt','r')
#api_key = api_file.read()
#api_file.close()

api_key = '[Insertar su api key]' #Lo utilizamos de esta manera a los fines de la demostracion

#Establecemos las variables para que el pasajero pueda introducir su punto de origen y destino
origin = input('\nPunto de origen:')
destiny = input('\nPunto de Destino:')

# base url para hacer los requests
url = "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&"

# get response
r = requests.get(url + "origins=" + origin + "&destinations=" + destiny + "&key=" + api_key)

#Lo pasamos a formato JSON, para obtener los datos que son de nuestros interes
r.json()

 
# extraemos el tiempo y la distancia
time = r.json()["rows"][0]["elements"][0]["duration"]["text"]       
distance = r.json()["rows"][0]["elements"][0]["distance"]["text"]
  
distance = float(distance[:4])
time = int(time[:2])

########################################################################################################################

# Modelo de Machine Learning
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn import tree
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from datetime import datetime

#Utilizamos las columnas que nos aportan informacion importante al modelo
columns=['tpep_pickup_datetime',
           'trip_distance',
           'Travel_time',
           'total_amount',
           'Outlier']

# Cargamos el csv correspondiente a los viajes
data = pd.read_csv(r'C:\Users\54266\Desktop\Henry\Proyectos\TrabajoGrupal-TaxisNYC\week-2\Data\df1.csv', usecols=columns)
#Aplicamos una mascara para obtener datos sin outliers, obteniendo solo los datos con etiqueta '0'
mask = data['Outlier'] == 0
data1 = data[mask]
data1 = data1[['tpep_pickup_datetime',
           'trip_distance',
           'Travel_time',
           'total_amount',]]

#Obtenemos el mes, dia del mes, dia de la semana y hora para que el modelo aprenda sobre los patrones de viajes segun la fecha
#Ademas nuestra fecha original esta en formato datatime y el modelo solo entiende en formato numerico
data1['tpep_pickup_datetime']  = pd.to_datetime(data1['tpep_pickup_datetime'])
data1['month'] = pd.DatetimeIndex(data1['tpep_pickup_datetime']).month
data1['day'] = pd.DatetimeIndex(data1['tpep_pickup_datetime']).day
data1['dayofweek'] = pd.DatetimeIndex(data1['tpep_pickup_datetime']).dayofweek
data1['hour'] = pd.DatetimeIndex(data1['tpep_pickup_datetime']).hour

#Dividimos nuestros datos en Entrenamiento y prueba
X = data1[['month',
            'day',
            'dayofweek',
            'hour',
           'trip_distance',
           'Travel_time'
           ]]
y = data1['total_amount']
X_train, X_test, y_train, y_test= train_test_split(X,y,test_size=0.2)

#Creamos el modelo. En este caso sera una regression lineal
#print('Entrenamiento modelo 0/3 (Creando modelo)')
LReg = LinearRegression()

#print('Entrenamiento modelo 1/3 (Adecuando modelo)')
LReg.fit(X_train, y_train)

#print('Entrenamiento modelo 2/3 (Entrenando modelo)')
LReg_y_pred = LReg.predict(X_test)

#print('Entrenamiento modelo 3/3 terminado!           ')

#print('Puntuacion del modelo:',LReg.score(X_test,y_test))

#######################################################################################################################

#Implementacion final para obtener el resultado de la tarifa

today = datetime.today() #obtenemos la fecha actual y hacemos el proceso anterior de descomposicion en formato numerico
month = today.month
day = today.day
dayofweek = today.weekday()
hour = today.hour
prueba = {'month':[month],
            'day':[day],
            'dayofweek': [dayofweek],
            'hour':[hour],
            'trip_distance': [distance],
            'Travel_time':[time]}
prueba_final = pd.DataFrame(prueba)


LReg_prueba = LinearRegression()
LReg_prueba.fit(X_train, y_train)
print('\nEstimando Viaje...')
LReg_prueba_pred = LReg_prueba.predict(prueba_final)

# Imprimimos el resultado final
print("\nDistancia de viaje:", distance ,'millas')
print("\nTiempo aproximado de viaje", time, 'minutos')
print('\nPrecio estimado de viaje: $',int(LReg_prueba_pred))
print('\n Disclaimer: este precio no representa el costo total, ni incluye sobrecargos por aeropuertos o peajes.')