from src.dao import DAO
from pyspark.sql import functions as f
from pyspark.sql.window import Window


def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("class")
    dim_class = dao.load("int_adult").select("class").distinct()
    dim_class = (dim_class
                    .withColumn("id_class", 
                                f.row_number().over(w))
                    .withColumn("id_class", f.when(f.col("class") == "NA", 
                                                -1)
                                            .otherwise(f.col("id_class")))
                    .select("id_class", "class")
                    .orderBy(f.col("id_class")))

    dao.save(dim_class, "bs_dim_class", save_mode="append")

