from pathlib import Path

from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
import pandas as pd


def aniade_hora_utc(spark: SparkSession, df: DF) -> DF:
    """
    Adds the flight's UTC timestamp in a 'FlightTime' column.

    Rules:
    - Joins by origin IATA against a local catalog `resources/timezones.csv`
      with columns: iata_code, iana_tz (IANA timezone).
    - Constructs local time from FlightDate (yyyy-MM-dd) + DepTime (HHmm),
      tolerating DepTime values of 3 or 4 digits (padded with '0' on the left).
    - Converts to UTC with `to_utc_timestamp`.

    :param spark: Active SparkSession.
    :param df: DataFrame with columns 'FlightDate' (date/string), 'DepTime' (int/string HHmm) and 'Origin' (IATA).
    :return: DF with new column 'FlightTime' in UTC (timestamp). If there is no timezone,
             'FlightTime' will be NULL.
    """
    path_timezones = str(Path(__file__).parent / "resources" / "timezones.csv")
    timezones_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path_timezones)
        .select("iata_code", "iana_tz")
    )

    df_with_tz = df.join(timezones_df, df["Origin"] == timezones_df["iata_code"], "left")

    dep_str = F.lpad(F.col("DepTime").cast("string"), 4, "0")
    
    flight_time_str = F.concat(
        F.col("FlightDate").cast("string"),
        F.lit(" "),
        dep_str.substr(1, 2),  # HH
        F.lit(":"),
        dep_str.substr(3, 2),  # mm
    )

    df_local = df_with_tz.withColumn("FlightTime", F.to_timestamp(flight_time_str, "yyyy-MM-dd HH:mm"))

    df_utc = df_local.withColumn(
        "FlightTime",
        F.expr("to_utc_timestamp(FlightTime, iana_tz)")
    )

    return df_utc.drop("iata_code").drop("iana_tz")   


def aniade_intervalos_por_aeropuerto(df: DF) -> DF:
    """
    Adds information about the next flight by origin airport, ordered by 'FlightTime'.

    Added columns:
      - FlightTime_next: timestamp of the next flight from the same 'Origin'
      - Airline_next:    airline code of the next flight
      - diff_next:       difference in seconds between FlightTime_next and FlightTime
                         (int, NULL if there is no next flight on the day)

    :param df: DataFrame with columns 'Origin', 'FlightTime' and 'Reporting_Airline'.
    :return: DF with previous columns added.
    """
    w = Window.partitionBy("Origin").orderBy(F.col("FlightTime").asc())

    out = (
        df
        .withColumn("FlightTime_next", F.lead("FlightTime").over(w))
        .withColumn("Airline_next", F.lead("Reporting_Airline").over(w))
        .withColumn(
            "diff_next",
            (F.col("FlightTime_next").cast("long") - F.col("FlightTime").cast("long")).cast("bigint")
        )
    )
    return out

