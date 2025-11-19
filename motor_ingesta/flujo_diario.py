import json
from datetime import timedelta
from loguru import logger

from pyspark.sql import SparkSession, functions as F

from .motor_ingesta import MotorIngesta
from .agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto

class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Loads the configuration and obtains/creates the SparkSession.

        :param config_file: Path to a JSON file with, at least, the keys:
            - "output_table": name of the external table where to write (string)
            - "output_path":  storage path for the external table (string)
            - "output_partitions": number of partitions when writing (int)
            - Any other key used by the ingestion engine (formats, schemas, etc.)
        """
        self.spark = SparkSession.builder.getOrCreate()

        with open(config_file, "r", encoding="utf-8") as f:
            self.config = json.load(f)

    def procesa_diario(self, data_file: str):
        """
        Orchestrates the daily processing and writes the external table partitioned by FlightDate.

        :param data_file: Path to the day's JSON file.
        """
        try:

            motor_ingesta = MotorIngesta(self.config)
            flights_df = motor_ingesta.ingesta_fichero(data_file).cache()

            flights_with_utc = aniade_hora_utc(self.spark, flights_df)

            dia_actual = flights_with_utc.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)
            try:
                flights_previo = self.spark.read.table(self.config["output_table"]).where(F.col("FlightDate") == dia_previo)
                logger.info(f"Successfully read partition for day {dia_previo}")
            except Exception as e:
                logger.info(f"Could not read data for day {dia_previo}: {str(e)}")
                flights_previo = None

            if flights_previo:
                df_unido = flights_previo.unionByName(flights_with_utc, allowMissingColumns=True)

                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")

            else:
                df_unido = flights_with_utc

            df_with_next_flight = aniade_intervalos_por_aeropuerto(df_unido)
            
            (
            df_with_next_flight
                .repartition(int(self.config.get("output_partitions", 1)), F.col("FlightDate"))
                .write
                .mode("overwrite")
                .option("partitionOverwriteMode", "dynamic")
                .partitionBy("FlightDate")
                .saveAsTable(self.config["output_table"])
            )

            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

        except Exception as e:
            logger.error(f"Could not write table for file {data_file}")
            raise e


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    ruta_config = "../config/config.json"
    ruta_fichero = "../data/landing/2023-01-01.json"

    flujo = FlujoDiario(ruta_config)
    flujo.procesa_diario(ruta_fichero)