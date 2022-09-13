from src.dao import DAO
from pyspark.sql import functions as f
from pyspark.sql.window import Window

def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("relationship")
    dim_relationship = dao.load("int_adult").select("relationship").distinct()
    dim_relationship = (dim_relationship.select("relationship")
                    .withColumn("id_relationship", 
                                f.row_number().over(w))
                    .withColumn("id_relationship", 
                                f.when(f.col("relationship") == "NA", 
                                                -1)
                                 .otherwise(f.col("id_relationship")))
                    .select("id_relationship", "relationship"))
    dao.save(dim_relationship, "bs_dim_relationship", save_mode="append")

