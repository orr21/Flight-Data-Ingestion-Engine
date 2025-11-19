import json
from collections import namedtuple
from pathlib import Path
from motor_ingesta.motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_intervalos_por_aeropuerto, aniade_hora_utc
from pyspark.sql import functions as F
import datetime


def test_aplana(spark):
    """
    Tests that flattening is done correctly with an ad-hoc DF
    :param spark: Locally configured SparkSession
    :return:
    """
    tupla3 = namedtuple("tupla3", ["a1", "a2", "a3", "a4"])
    tupla2 = namedtuple("tupla2", ["b1", "b2"])

    test_df = spark.createDataFrame(
        [(tupla3("a", "b", "c", "d"), "hola", 3, [tupla2("pepe", "juan"), tupla2("pepito", "juanito")])],
        ["tupla", "nombre", "edad", "amigos"]
    )

    aplanado_df = MotorIngesta.aplana_df(test_df)

    assert set(aplanado_df.columns) == {"a1", "a2", "a3", "a4", "nombre", "edad", "b1", "b2"}

def test_ingesta_fichero_json(spark):
    """
    Checks that the ingestion of a test JSON file is done correctly. Uses the
    existing JSON file in the tests/resources folder
    :param spark: Locally initialized SparkSession
    :return:
    """

    carpeta_este_fichero = str(Path(__file__).parent)
    path_test_config = carpeta_este_fichero + "/resources/test_config.json"
    path_test_data = carpeta_este_fichero + "/resources/test_data.json"

    with open(path_test_config) as f:
        config = json.load(f)

    motor_ingesta = MotorIngesta(config)

    datos_df = motor_ingesta.ingesta_fichero(path_test_data)

    assert datos_df.count() == 1
    assert set(datos_df.columns) == {"nombre", "parentesco", "numero", "profesion"}

    primera_fila = datos_df.first()
    assert primera_fila.nombre == "Juan"
    assert primera_fila.parentesco == "sobrino"
    assert primera_fila.numero == 3
    assert primera_fila.profesion == "Ingeniero"
    
def test_ingesta_fichero(spark):
    """
    Checks that the ingestion of a test CSV file is done correctly. Uses the
    CSV file existing in the tests/resources folder
    :param spark: Locally initialized SparkSession
    :return:
    """

    carpeta_este_fichero = str(Path(__file__).parent)
    path_test_config = carpeta_este_fichero + "/resources/test_config.json"
    path_test_data = carpeta_este_fichero + "/resources/test_data.json"

    with open(path_test_config) as f:
        config = json.load(f)

    motor_ingesta = MotorIngesta(config)

    datos_df = motor_ingesta.ingesta_fichero(path_test_data)

    assert datos_df.count() == 1
    assert set(datos_df.columns) == {"nombre", "parentesco", "numero", "profesion"}

    primera_fila = datos_df.first()
    assert primera_fila.nombre == "Juan"
    assert primera_fila.parentesco == "sobrino"
    assert primera_fila.numero == 3
    assert primera_fila.profesion == "Ingeniero"

def test_aniade_intervalos_por_aeropuerto(spark):
    """
    Checks that the added variables with information about the immediately following flight departing from the same
    airport are calculated correctly
    :param spark: Locally initialized SparkSession
    :return:
    """

    test_df = spark.createDataFrame(
        [("JFK", "2023-12-25 15:35:00", "American_Airlines"),
         ("JFK", "2023-12-25 17:35:00", "Iberia")],
        ["Origin", "FlightTime", "Reporting_Airline"]
    ).withColumn("FlightTime", F.col("FlightTime").cast("timestamp"))

    expected_df = spark.createDataFrame(
    [("JFK", "2023-12-25 15:35:00", "American_Airlines", "2023-12-25 17:35:00", "Iberia", 7200)],
    ["Origin", "FlightTime", "Reporting_Airline", "FlightTime_next", "Airline_next", "diff_next"]
    ).withColumn("FlightTime", F.col("FlightTime").cast("timestamp")
    ).withColumn("FlightTime_next", F.col("FlightTime_next").cast("timestamp"))

    expected_row = expected_df.first()

    result_df = aniade_intervalos_por_aeropuerto(test_df)
    actual_row = result_df.first()

    assert expected_row.Origin == actual_row.Origin
    assert expected_row.FlightTime == actual_row.FlightTime
    assert expected_row.Reporting_Airline == actual_row.Reporting_Airline
    assert expected_row.FlightTime_next == actual_row.FlightTime_next
    assert expected_row.Airline_next == actual_row.Airline_next
    assert expected_row.diff_next == actual_row.diff_next

def test_aniade_hora_utc(spark):
    """
    Checks that the FlightTime column in the UTC timezone is correctly calculated
    :param spark: Locally initialized SparkSession
    :return:
    """

    fichero_timezones = str(Path(__file__).parent) + "../motor_ingesta/resources/timezones.csv"

    test_df = spark.createDataFrame(
        [("JFK", "2023-12-25", 1535)],
        ["Origin", "FlightDate", "DepTime"]
    )

    expected_df = spark.createDataFrame(
        [("JFK", "2023-12-25", 1535, datetime.datetime(2023, 12, 25, 20, 35, 0))],
        ["Origin", "FlightDate", "DepTime", "FlightTime"]
    )

    expected_row = expected_df.first()

    result_df = aniade_hora_utc(spark, test_df)
    actual_row = result_df.first()

    assert(expected_row.Origin == actual_row.Origin)
    assert(expected_row.FlightDate == actual_row.FlightDate)
    assert(expected_row.DepTime == actual_row.DepTime)
    assert(expected_row.FlightTime == actual_row.FlightTime)
