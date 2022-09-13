from src.dao import DAO
from pyspark.sql.window import Window
from pyspark.sql import functions as f

def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("marital-status")
    dim_m_status = dao.load("int_adult").select("marital-status").distinct()
    dim_m_status = (dim_m_status.select("marital-status")
                    .withColumn("id_marital-status", 
                                f.row_number().over(w))
                    .withColumn("id_marital-status", 
                                f.when(f.col("marital-status") == "NA", 
                                                -1)
                                .otherwise(f.col("id_marital-status")))
                    .select("id_marital-status", "marital-status"))
    dao.save(dim_m_status, "bs_dim_marital_status", save_mode="append")

