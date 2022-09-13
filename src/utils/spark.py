import json
import pathlib
import findspark
from src.utils import Spark
from pyspark.sql.types import *
findspark.init()
from pyspark.sql import SparkSession 

PATH_PROJECT = str(pathlib.Path(__file__).parent.parent.absolute())
PATH_RESOURCES = f"{PATH_PROJECT}/data/"
PATH_PARAMETERS = F"{PATH_PROJECT}/utils/parameters.json"


schema = (
            StructType(
                    [
                        StructField("age", IntegerType(), False),
                        StructField("workclass", StringType() , False),
                        StructField("fnlwgt", FloatType(), False),
                        StructField("education", StringType(), False),
                        StructField("education-num", FloatType(), False),
                        StructField("marital-status", StringType(), False),
                        StructField("occupation", StringType(), False),
                        StructField("relationship", StringType(), False),
                        StructField("race", StringType(), False),
                        StructField("sex", StringType(), False),
                        StructField("capital-gain", FloatType(), False),
                        StructField("capital-loss", FloatType(), False),
                        StructField("hours-per-week", FloatType(), False),
                        StructField("native-country", StringType(), False),
                        StructField("class", StringType(), False)
                    ])
         )


def schema_from_json(filename):
    with open(filename) as fjson:
        return StructType.fromJson(json.loads(fjson.read()))


def schema_from_resources(path_name):
    return schema_from_json(f"{PATH_RESOURCES}/{path_name}")


def new_dataframe():
    spark = Spark().spark
    return (spark.read.options(sep=",")
                      .options(header=False)
                      .schema(schema)
                      .csv(f"{PATH_RESOURCES}"))

def return_json():
    with open(PATH_PARAMETERS) as json_file:
        return json.load(json_file)