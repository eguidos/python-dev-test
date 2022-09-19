from pyspark.sql.window import Window
from src.dao import DAO
from pyspark.sql import functions as f

def execute():
    dao = DAO()
    w = Window().partitionBy().orderBy("native-country")
    dim_native_country = dao.load("int_adult").select("native-country").distinct()
    dim_native_country = (dim_native_country.select("native-country")
                    .withColumn("id_native-country", 
                                f.row_number().over(w))
                    .withColumn("id_native-country", 
                                f.when(f.col("native-country") == "NA", 
                                                -1)
                                 .otherwise(f.col("id_native-country")))
                    .select("id_native-country", "native-country"))
    dao.save(dim_native_country, "bs_dim_native_country", save_mode="append")