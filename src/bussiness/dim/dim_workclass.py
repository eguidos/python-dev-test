from pyspark.sql.window import Window
from src.dao import DAO
from pyspark.sql import functions as f

def execute(write=False):
    dao = DAO()
    w = Window().partitionBy().orderBy("workclass")
    dim_workclass = dao.load("int_adult").select("workclass").distinct()
    dim_workclass = (dim_workclass.select("workclass")
                    .withColumn("id_workclass", 
                                f.row_number().over(w))
                    .withColumn("id_workclass", f.when(f.col("workclass") == "NA", 
                                                -1)
                                            .otherwise(f.col("id_workclass")))
                    .select("id_workclass", "workclass"))
    dao.save(dim_workclass, "bs_dim_workclass", save_mode="append")
