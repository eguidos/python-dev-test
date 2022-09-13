from src.dao import DAO
from pyspark.sql import functions as f
from pyspark.sql.window import Window

def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("age")
    dim_age = dao.load("int_adult").select("age").distinct()
    dim_age = (dim_age
                    .withColumn("id_age", 
                                f.row_number().over(w))
                    .withColumn("id_age", f.when(f.col("age") == "NA", 
                                                -1)
                                            .otherwise(f.col("id_age")))
                    .select("id_age", "age")
                    .orderBy(f.col("id_age")))
    return dao.save(dim_age, "bs_dim_age", save_mode="append")

