from pyspark.sql.window import Window
from src.dao import DAO
from pyspark.sql import functions as f

def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("race")
    dim_race = dao.load("int_adult").select("race").distinct()
    dim_race = (dim_race.select("race")
                    .withColumn("id_race", 
                                f.row_number().over(w))
                    .withColumn("id_race", 
                                f.when(f.col("race") == "NA", 
                                                -1)
                                 .otherwise(f.col("id_race")))
                    .select("id_race", "race"))
    dao.save(dim_race, "bs_dim_race", save_mode="append")

