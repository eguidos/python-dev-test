from pyspark.sql.window import Window
from src.dao import DAO
from pyspark.sql import functions as f

def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("occupation")
    dim_occupation = dao.load("int_adult").select("occupation").distinct()
    dim_occupation = (dim_occupation.select("occupation")
                    .withColumn("id_occupation", 
                                f.row_number().over(w))
                    .withColumn("id_occupation", 
                                f.when(f.col("occupation") == "NA", 
                                                -1)
                                 .otherwise(f.col("id_occupation")))
                    .select("id_occupation", "occupation"))
    dao.save(dim_occupation, "bs_dim_occupation", save_mode="append")
