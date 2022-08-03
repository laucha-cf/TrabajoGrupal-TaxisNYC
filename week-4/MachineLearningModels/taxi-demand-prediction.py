import time
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import MiniBatchKMeans, KMeans
import gpxpy.geo # Get the haversine distance
from sklearn.linear_model import LinearRegression
from sklearn import tree
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
import math
from prettytable import PrettyTable

data1 = pd.read_csv(r'C:\Users\54266\Desktop\Henry\Proyectos\TrabajoGrupal-TaxisNYC\week-3\MachineLearning\data_procesada.csv')

data2 = data1.copy()


print("Obteniendo mes, dias, horas y dia de la semana")
data2['month'] = pd.DatetimeIndex(data2['pickup_time']).month
data2['day'] = pd.DatetimeIndex(data2['pickup_time']).day
data2['dayofweek'] = pd.DatetimeIndex(data2['pickup_time']).dayofweek
data2['hour'] = pd.DatetimeIndex(data2['pickup_time']).hour

print("Agrupando por TLC Zones y tiempo")
data2 = data2.groupby(['dayofweek','hour','PULocationID']).size().reset_index(name='Probabilidad')


print("Convirtiendo 'Probabilidad' a porcentaje de Demanda")
#pandarallel.initialize()
#data2['Probabilidad'] = data2['Probabilidad'].parallel_apply(lambda x :  (x / data2['Probabilidad'].max()))
data2['Probabilidad'] = data2['Probabilidad'].apply(lambda x :  (x / data2['Probabilidad'].max()))
data2['Probabilidad'] = round(data2['Probabilidad']*100,2)

top_zones = data2.sort_values(by='Probabilidad', ascending=False)
top_zones.head()

# training X and y
X = data2[['dayofweek','hour','PULocationID']]
y = data2['Probabilidad']


from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test= train_test_split(X,y,test_size=0.3)

# Regression lineal
print('\n Regresion Lineal:')
print('model training 0/3 (Creando modelo)', end='\r')
LReg = LinearRegression()

print('model training 1/3 (Adecuando modelo)', end='\r')
LReg.fit(X_train, y_train)

print('model training 2/3 (Entrenando modelo)', end='\r')
LReg_y_pred = LReg.predict(X_test)

print('model training 3/3 done!           ', end='\r')

print('Puntuacion del modelo:',LReg.score(X_test,y_test))

# Random forest
print('\n Random Forest:')
print('model training 0/3 (Creando modelo)', end='\r')
RFRegr = RandomForestRegressor()

print('model training 1/3 (Adecuando modelo)', end='\r')
RFRegr.fit(X_train, y_train)

print('model training 2/3 (Entrenando modelo)', end='\r')
RFRegr_y_pred = RFRegr.predict(X_test)

print('model training 3/3 done!           ', end='\r')

print('Puntuacion del modelo:',RFRegr.score(X_test,y_test))

# XGBregressor
print('\n XGBregressor:')
print('model training 0/3 (Creando modelo)', end='\r')
GBRegr = XGBRegressor(n_estimators=1000, max_depth=7, eta=0.1, subsample=0.7, colsample_bytree=0.8)

print('model training 1/3 (Adecuando modelo)', end='\r')
GBRegr.fit(X_train, y_train)

print('model training 2/3 (Entrenando modelo)', end='\r')
GBRegr_y_pred = GBRegr.predict(X_test)

print('model training 3/3 done!           ', end='\r')

print('Puntuacion del modelo:',GBRegr.score(X_test,y_test))

#la idea es hacer la prediccion con el random forest y despues cuando el chofer este sin un viaje, 
#debe introducir dia de la semana y horario; y le mostrara los 10 lugares con mas probabilidades de obtener un viaje

#Primero obtener la zona, horario y dia que este operando el taxi
zona_actual = input('Introduzca zona actual:')
fecha_actual = datetime.today() #obtenemos la fecha actual
hora = fecha_actual.hour #hora
dia = fecha_actual.day #dia del mes
dia_semana = fecha_actual.weekday()  #dia de la semana (Lunes = 0 ... domingo = 6)

#ahora predecir la probabilidad de encontrar un viaje con los paremetros marcados

localizacion_chofer = {
        'dayofweek': [dia_semana],
        'hour':[hora],
        'PULocationID':zona_actual}
prueba_final = pd.DataFrame(localizacion_chofer)

RForest_test = RandomForestRegressor()
RForest_test.fit(X_train, y_train)
print('Aplicando Modelo')
RForest_test_pred = RForest_test.predict(prueba_final)

print('Probabilidad de encontrar un viaje en esta zona %',round(RForest_test_pred[0]))

recomendacion = top_zones.query('dayofweek == @dia_semana & hour == @hora')
zonesID = pd.read_csv(r'C:\Users\54266\Desktop\Henry\Proyectos\TrabajoGrupal-TaxisNYC\week-2\Data\taxi+_zone_lookup.csv')
r = recomendacion.merge(zonesID, left_on='PULocationID', right_on ='LocationID')

print('\n Por favor diríjase a las siguientes zonas:')
print(r.head(10))



