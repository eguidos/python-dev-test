from src.dao import DAO
from pyspark.sql.window import Window
from pyspark.sql import functions as f

def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("education")
    dim_education = dao.load("int_adult").select("education").distinct()
    dim_education = (dim_education.select("education")
                    .withColumn("id_education", 
                                f.row_number().over(w))
                    .withColumn("id_education", f.when(f.col("education") == "NA", 
                                                -1)
                                            .otherwise(f.col("id_education")))
                    .select("id_education", "education"))
    dao.save(dim_education, "bs_dim_education", save_mode="append")