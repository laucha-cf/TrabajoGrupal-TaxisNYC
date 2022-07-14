# -- Librerías usadas -- #
import os

from sqlalchemy import create_engine, Column, Integer, DECIMAL, String, Date, Time
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

#  -- CONSTANTES -- #
DBMS = 'postgresql'
USER = os.environ.get('DB_USER')
PASSWORD = os.environ.get('DB_PASSWORD')
HOST = os.environ.get('DB_ENDPOINT')
PORT = '5432'
DB_NAME = os.environ.get('DB_NAME')

# -- Creación del engine y de la db -- #
engine = create_engine(f'{DBMS}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
if not database_exists(engine.url):
    create_database(engine.url)

# -- Instanciamos la sesión -- #
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base() #  Usada para declarar y crear las tablas

# -- Declaración de tablas -- #
class Trip(Base):
    __tablename__ = 'Trip'

    IdTrip = Column(Integer, primary_key=True)
    IdVendor = Column(Integer)
    Date = Column(Date)
    PU_Time = Column(Time)
    Duration = Column(Integer)
    Passenger_Count = Column(Integer)
    Distance = Column(DECIMAL(5,2))
    PU_IdZone = Column(Integer)
    DO_IdZone = Column(Integer)
    Temperature = Column(DECIMAL(4,2))
    IdPrecip_Type = Column(Integer)



class Payment(Base):
    __tablename__ = 'Payment'

    IdPayment = Column(Integer, primary_key=True)
    IdTrip = Column(Integer)
    IdRate_Code = Column(Integer)
    IdPayment_Type = Column(Integer)
    Fare_Amount = Column(DECIMAL(6,2))
    Extra = Column(DECIMAL(4,2))
    MTA_Tax = Column(DECIMAL(4,2))
    Improvement_Surcharge = Column(DECIMAL(4,2))
    Tolls_Amount = Column(DECIMAL(5,2))
    Total_Amount = Column(DECIMAL(6,2))


class Vendor(Base):
    __tablename__ = 'Vendor'

    IdVendor = Column(Integer, primary_key=True)
    Vendor = Column(String(80))


class Borough(Base):
    __tablename__ = 'Borough'

    IdBorough = Column(Integer, primary_key=True)
    Borough = Column(String(50))


class Service_Zone(Base):
    __tablename__ = 'Service_Zone'

    IdService_Zone = Column(Integer, primary_key=True)
    Service_Zone = Column(String(50))


class Precip_Type(Base):
    __tablename__ = 'Precip_Type'

    IdPrecip_Type = Column(Integer, primary_key=True)
    Precip_Type = Column(String(50))


class Rate_Code(Base):
    __tablename__ = 'Rate_Code'

    IdRate_Code = Column(Integer, primary_key=True)
    Rate_Code = Column(String(50))


class Payment_Type(Base):
    __tablename__ = 'Payment_Type'

    IdPayment_Type = Column(Integer, primary_key=True)
    Payment_Type = Column(String(50))


class Calendar(Base):
    __tablename__ = 'Calendar'

    IdDate = Column(Integer, primary_key=True)
    Date = Column(Date)
    Year = Column(Integer)
    Month = Column(Integer)
    Day = Column(Integer)
    Week = Column(Integer)
    Day_of_Week = Column(String(50))


class Zone(Base):
    __tablename__ = 'Zone'

    IdZone = Column(Integer, primary_key=True)
    Zone = Column(String(80))
    IdBorough = Column(Integer)
    IdService_Zone = Column(Integer)


# -- Creación de tablas en la DB -- #
Base.metadata.create_all(engine)
