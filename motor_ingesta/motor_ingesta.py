
import json
from pyspark.sql import DataFrame as DF, functions as F, SparkSession


class MotorIngesta:
    """
    Ingestion engine for the daily flight flow.

    - Reads JSON files with schema inference.
    - Flattens arrays and structures (arbitrary nesting).
    - Selects and casts final columns according to `config["data_columns"]`.
    """
    def __init__(self, config: dict):
        """
        Initializes the engine with the configuration and creates/retrieves the SparkSession.

        :param config: Configuration dictionary for the flow. Must include
                       at least the "data_columns" key to map names, types, and comments.
        """
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def ingesta_fichero(self, json_path: str) -> DF:
        """
        Ingests a JSON file, flattens its structure, and returns a DataFrame
        with columns cast and named according to `config["data_columns"]`.

        :param json_path: Path to the input JSON file (can be local path or data lake).
        :return: DataFrame with the final schema required by the flow.
        """
        flights_day_df = (
            self.spark.read
            .option("inferSchema", "true")
            .json(json_path)
        )

        aplanado_df = self.aplana_df(flights_day_df)
        lista_obj_column = [
            F.col(diccionario["name"]).cast(diccionario["type"]).alias(diccionario["name"], metadata={"comment": diccionario["comment"]}) 
            for diccionario in self.config["data_columns"]
        ]
        resultado_df = aplanado_df.select(lista_obj_column)
        return resultado_df


    @staticmethod
    def aplana_df(df: DF) -> DF:
        """
        Flattens a Spark DataFrame that has array and struct type columns.

        :param df: Spark DataFrame containing array or struct type columns, including
                   any level of nesting and also arrays of structures. We assume that the names of the
                   nested fields are all distinct from each other, and will not coincide when flattened.
        :return: Spark DataFrame where all array type columns have been exploded and structures
                 have been recursively flattened.
        """
        to_select = []
        schema = df.schema.jsonValue()
        fields = schema["fields"]
        recurse = False

        for f in fields:
            if f["type"].__class__.__name__ != "dict":
                to_select.append(f["name"])
            else:
                if f["type"]["type"] == "array":
                    to_select.append(F.explode(f["name"]).alias(f["name"]))
                    recurse = True
                elif f["type"]["type"] == "struct":
                    to_select.append(f"{f['name']}.*")
                    recurse = True

        new_df = df.select(*to_select)
        return MotorIngesta.aplana_df(new_df) if recurse else new_df
