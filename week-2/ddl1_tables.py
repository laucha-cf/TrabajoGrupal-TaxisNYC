# -- Librerías usadas -- #
import os

from sqlalchemy import create_engine, Column, ForeignKey, Integer, DECIMAL, String, Date, Time
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy_utils import database_exists, create_database

#  -- CONSTANTES -- #
DBMS = 'postgresql'
USER = 'postgres'
PASSWORD = 'shakejunt02'
HOST = 'localhost'
PORT = '5432'
DB_NAME = 'g9'

# -- Creación del engine y de la db -- #
engine = create_engine(f'{DBMS}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
if not database_exists(engine.url):
    create_database(engine.url)

# -- Instanciamos la sesión -- #
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()  # Usada para declarar y crear las tablas


# -- Declaración de tablas -- #
class ServiceZone(Base):
    __tablename__ = 'Service_Zone'
    IdService_Zone = Column(Integer, primary_key=True)
    Service_Zone = Column(String(50))
    zones = relationship('Zone')


class Borough(Base):
    __tablename__ = 'Borough'
    IdBorough = Column(Integer, primary_key=True)
    Borough = Column(String(50))
    zones = relationship('Zone')


class Zone(Base):
    __tablename__ = 'Zone'
    IdZone = Column(Integer, primary_key=True)
    Zone = Column(String(80))
    IdBorough = Column(Integer, ForeignKey('Borough.IdBorough'))
    IdService_Zone = Column(Integer, ForeignKey('Service_Zone.IdService_Zone'))
    trips = relationship('Trip')


class Vendor(Base):
    __tablename__ = 'Vendor'
    IdVendor = Column(Integer, primary_key=True)
    Vendor = Column(String(80))
    trips = relationship('Trip')


class Calendar(Base):
    __tablename__ = 'Calendar'
    IdDate = Column(Integer, primary_key=True)
    Date = Column(Date)
    Year = Column(Integer)
    Month = Column(Integer)
    Day = Column(Integer)
    Week = Column(Integer)
    Day_of_Week = Column(String(50))
    trips = relationship('Trip')


class PrecipType(Base):
    __tablename__ = 'Precip_Type'
    IdPrecip_Type = Column(Integer, primary_key=True)
    Precip_Type = Column(String(50))
    trips = relationship('Trip')


class Trip(Base):
    __tablename__ = 'Trip'
    IdTrip = Column(Integer, primary_key=True)
    IdVendor = Column(Integer, ForeignKey('Vendor.IdVendor'))
    IdDate = Column(Integer, ForeignKey('Calendar.IdDate'))
    PU_Time = Column(Time)
    Duration = Column(Integer)
    Passenger_Count = Column(Integer)
    Distance = Column(DECIMAL(5, 2))
    PU_IdZone = Column(Integer, ForeignKey('Zone.IdZone'))
    DO_IdZone = Column(Integer, ForeignKey('Zone.IdZone'))
    Temperature = Column(DECIMAL(4, 2))
    IdPrecip_Type = Column(Integer, ForeignKey('Precip_Type.IdPrecip_Type'))
    payments = relationship('Payment', uselist=False)


class RateCode(Base):
    __tablename__ = 'Rate_Code'
    IdRate_Code = Column(Integer, primary_key=True)
    Rate_Code = Column(String(50))
    payments = relationship('Payment')


class PaymentType(Base):
    __tablename__ = 'Payment_Type'
    IdPayment_Type = Column(Integer, primary_key=True)
    Payment_Type = Column(String(50))
    payments = relationship('Payment')


class Payment(Base):
    __tablename__ = 'Payment'
    IdPayment = Column(Integer, primary_key=True)
    IdTrip = Column(Integer, ForeignKey('Trip.IdTrip'))
    IdRate_Code = Column(Integer, ForeignKey('Rate_Code.IdRate_Code'))
    IdPayment_Type = Column(Integer, ForeignKey('Payment_Type.IdPayment_Type'))
    Fare_Amount = Column(DECIMAL(6, 2))
    Extra = Column(DECIMAL(4, 2))
    MTA_Tax = Column(DECIMAL(4, 2))
    Improvement_Surcharge = Column(DECIMAL(4, 2))
    Tolls_Amount = Column(DECIMAL(5, 2))
    Total_Amount = Column(DECIMAL(6, 2))
    trips = relationship('Trip')


# -- Creación de tablas en la DB -- #
Base.metadata.create_all(engine)
