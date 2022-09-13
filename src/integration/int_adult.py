from src.dao import DAO
from src.utils.spark import return_json
from pyspark.sql import functions as f

def find_values(col):
    data = return_json()
    return (f.coalesce(*[f.when(f.trim(f.col(col)).like(f"%{k}%"), k)
                         .when(f.col(col) == "NA",
                               "NA")
                         for k in data[col]]))


def execute(write=False):
    dao = DAO()
    adult = dao.load("raw_adult")
    adult = (adult.select(
        [
            f.when((f.col(i).isNull()) | 
                (f.trim(f.col(i)) == "?"), 
            "NA")
        .otherwise(f.col(i)).alias(i)
         for i in adult.columns
        ]
    ))
    adult =  (adult
                .withColumn("class", find_values("class"))
                .withColumn("workklass", find_values("workclass"))
                .withColumn("education", find_values("education"))
                .withColumn("from_to_marital_status", find_values("marital-status"))
                .withColumn("from_to_occupation", find_values("occupation"))
                .withColumn("from_to_relationship", find_values("relationship"))
                .withColumn("race_", find_values("race"))
                .withColumn("sex", find_values("sex"))
                .withColumn("native-country", find_values("native-country"))

                .select(*adult.columns))
    dao.save(adult, "int_adult", save_mode="append", col_part="current_data")




