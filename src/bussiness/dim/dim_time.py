from pyspark.sql.window import Window
from src.dao import DAO
from pyspark.sql import functions as f

def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("current_data")
    dim_time = dao.load("int_adult").select("current_data").distinct()
    dim_time = (dim_time.select("current_data")
                    .withColumn("id_current_data", 
                                f.row_number().over(w))
                    .withColumn("id_current_data", 
                                f.when(f.col("id_current_data") == "NA", 
                                                -1)
                                 .otherwise(f.col("id_current_data")))
                    .select("id_current_data", "current_data"))
    dao.save(dim_time, "bs_dim_time", save_mode="append")