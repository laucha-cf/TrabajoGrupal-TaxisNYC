# -- Librerías usadas -- #
import os

from sqlalchemy import create_engine, Column, ForeignKey, Integer, DECIMAL, String, Date, Time, BIGINT
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy_utils import database_exists, create_database

#  -- CONSTANTES -- #
DBMS = 'postgresql'
USER = 'postgres'
PASSWORD = '4217796'
HOST = 'localhost'
PORT = '5432'
DB_NAME = 'nyc_taxis'

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
    __tablename__ = 'service_zone'
    idservice_zone = Column(Integer, primary_key=True)
    service_zone = Column(String(50))
    zones = relationship('zone')


class Borough(Base):
    __tablename__ = 'borough'
    idborough = Column(Integer, primary_key=True)
    borough = Column(String(50))
    zones = relationship('zone')


class Zone(Base):
    __tablename__ = 'zone'
    idzone = Column(Integer, primary_key=True)
    zone = Column(String(80))
    idborough = Column(Integer, ForeignKey('borough.idborough'))
    idservice_zone = Column(Integer, ForeignKey('service_zone.idservice_zone'))
    trips = relationship('trip')


class Vendor(Base):
    __tablename__ = 'vendor'
    idvendor = Column(Integer, primary_key=True)
    vendor = Column(String(80))
    trips = relationship('trip')


class Calendar(Base):
    __tablename__ = 'calendar'
    iddate = Column(Integer, primary_key=True)
    date = Column(Date, index=True)
    year = Column(Integer, index=True)
    month = Column(Integer, index=True)
    day = Column(Integer, index=True)
    week = Column(Integer)
    day_of_week = Column(String(50), index=True)
    trips = relationship('trip')


class PrecipType(Base):
    __tablename__ = 'precip_type'
    idprecip_type = Column(Integer, primary_key=True)
    precip_type = Column(String(50))
    trips = relationship('trip')


class Trip(Base):
    __tablename__ = 'trip'
    idtrip = Column(BIGINT, primary_key=True)
    idvendor = Column(BIGINT, ForeignKey('vendor.idvendor'))
    iddate = Column(Integer, ForeignKey('calendar.iddate'))
    pu_time = Column(Time)
    duration = Column(Integer)
    passenger_count = Column(Integer)
    distance = Column(DECIMAL(5, 2))
    pu_idzone = Column(Integer, ForeignKey('zone.idzone'))
    do_idzone = Column(Integer, ForeignKey('zone.idzone'))
    temperature = Column(DECIMAL(4, 2))
    idprecip_type = Column(Integer, ForeignKey('precip_type.idprecip_type'))
    payments = relationship('payment', uselist=False)


class RateCode(Base):
    __tablename__ = 'rate_code'
    idrate_code = Column(Integer, primary_key=True)
    rate_code = Column(String(50))
    payments = relationship('payment')


class PaymentType(Base):
    __tablename__ = 'payment_type'
    idpayment_type = Column(Integer, primary_key=True)
    payment_type = Column(String(50))
    payments = relationship('payment')


class Payment(Base):
    __tablename__ = 'payment'
    idpayment = Column(BIGINT, primary_key=True)
    idtrip = Column(BIGINT, ForeignKey('trip.idtrip'))
    idrate_code = Column(Integer, ForeignKey('rate_code.idrate_code'))
    idpayment_type = Column(Integer, ForeignKey('payment_type.idpayment_type'))
    fare_amount = Column(DECIMAL(6, 2))
    extra = Column(DECIMAL(4, 2))
    mta_tax = Column(DECIMAL(4, 2))
    improvement_surcharge = Column(DECIMAL(4, 2))
    tolls_amount = Column(DECIMAL(5, 2))
    total_amount = Column(DECIMAL(6, 2), index=True)
    trips = relationship('trip')


class Outlier(Base):
    __tablename__ = 'aux_outlier'
    idrecord = Column(BIGINT, primary_key=True)


# -- Creación de tablas en la DB -- #
Base.metadata.create_all(engine)

# Cerramos la conexión
engine.dispose()
