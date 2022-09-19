from src.dao import DAO
from src.utils.spark import new_dataframe  
from pyspark.sql import functions as f

def execute(write=False):
    adult =  new_dataframe()
    adult = adult.withColumn("current_data", f.current_timestamp())
    dao = DAO()

    dao.save(adult, "raw_adult", save_mode="overwrite", col_part="current_data")