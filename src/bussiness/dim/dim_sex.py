from pyspark.sql.window import Window
from src.dao import DAO
from pyspark.sql import functions as f

def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("sex")
    dim_sex = dao.load("int_adult").select("sex").distinct()
    dim_sex = (dim_sex.select("sex")
                    .withColumn("id_sex", 
                                f.row_number().over(w))
                    .withColumn("id_sex", 
                                f.when(f.col("sex") == "NA", 
                                                -1)
                                 .otherwise(f.col("id_sex")))
                    .select("id_sex", "sex"))
    dao.save(dim_sex, "bs_dim_sex", save_mode="append")
