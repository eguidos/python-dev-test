from src.bussiness.dim import (dim_age, dim_class, dim_education, 
                               dim_marital_status, dim_native_country, 
                               dim_occupation, dim_race, dim_relationship,
                               dim_sex, dim_workclass)
from src.dao import DAO
from src.utils.constraints import schema_business
from pyspark.sql import functions as f

def join_dim_class(adult, dim_class):
    """f.col("adult.class") == f.col("dim_class.class")
        returns id_class
    """
    return adult.alias("adult").join(
                        dim_class.alias("dim_class"), 
                        f.col("adult.class") == f.col("dim_class.class"),
                "inner"
                     )

def join_dim_education(adult, dim_education):
    """f.col("adult.education") == f.col("dim_education.education")
        returns id_education
    """
    return (adult.join(dim_education.alias("dim_education"),
                       f.col("adult.education") == f.col("dim_education.education"),
                    "inner"
    ))

def join_dim_marital_status(adult, dim_marital_status):
    """f.col("adult.marital-status") == f.col("dim_marital_status.marital-status")
    returns id_marital-status
"""
    return (
            adult.join(dim_marital_status.alias("dim_marital_status"),
                        f.col("adult.marital-status") == f.col("dim_marital_status.marital-status"),
                       "inner")
        )

def join_dim_n_country(adult, dim_native_country):
    """f.col("adult.native-country") == f.col("dim_native_country.native-country")
        returns id_native-country
    """
    return(
            adult.join(dim_native_country.alias("dim_native_country"),
                      f.col("adult.native-country") == f.col("dim_native_country.native-country"),
                "inner"
                      ))

def join_dim_occupation(adult, dim_occupation):
    """f.col("adult.occupation") == f.col("dim_occupation.occupation")
        returns id_occupation
    """
    return(
            adult.join(dim_occupation.alias("dim_occupation"),
                      f.col("adult.occupation") == f.col("dim_occupation.occupation"),
                    "inner")
        )


def join_dim_race(adult, dim_race):
    """f.col("adult.race") == f.col("dim_race.race"),
        returns id_race
    """
    return(
            adult.join(dim_race.alias("dim_race"),
                      f.col("adult.race") == f.col("dim_race.race"),
                    "inner")
        )

def join_dim_relationship(adult, dim_relationship):
    """f.col("adult.relationship") == f.col("dim_relationship.relationship"),
        returns id_relationship
    """
    return(
            adult.join(dim_relationship.alias("dim_relationship"),
                      f.col("adult.relationship") == f.col("dim_relationship.relationship"),
                    "inner")
        )

def join_dim_sex(adult, dim_sex):
    """f.col("adult.sex") == f.col("dim_sex.sex"),
        returns id_sex
    """
    return(
            adult.join(dim_sex.alias("dim_sex"),
                      f.col("adult.sex") == f.col("dim_sex.sex"),
                    "inner")
        )

def join_workclass(adult, dim_workclass):
    """f.col("adult.workclass") == f.col("dim_workclass.workclass"),
        returns id_workclass
    """
    return(
            adult.join(dim_workclass.alias("dim_workclass"),
                      f.col("adult.workclass") == f.col("dim_workclass.workclass"),
                    "inner")
        )

def join_dim_time(adult, dim_time):
    """f.col("adult.current_data") == f.col("bs_dim_time.current_data"),
        returns id_current_data
    """
    return(
            adult.join(dim_time.alias("dim_time"),
                      f.col("adult.current_data") == f.col("dim_time.current_data"),
                    "inner")
        )
  

def execute(write = False):
    dao = DAO()
    adult = dao.load("int_adult")
    dim_class = dao.load("bs_dim_class")
    dim_education = dao.load("bs_dim_education")
    dim_occupation = dao.load("bs_dim_occupation")
    dim_marital_status = dao.load("bs_dim_marital_status")
    dim_native_country = dao.load("bs_dim_native_country")
    dim_occupation = dao.load("bs_dim_occupation")
    dim_race = dao.load("bs_dim_race")
    dim_relationship = dao.load("bs_dim_relationship")
    dim_sex = dao.load("bs_dim_sex")
    dim_workclass = dao.load("bs_dim_workclass")
    dim_time = dao.load("bs_dim_time")


    adult = join_dim_class(adult, dim_class)
    adult = join_dim_education(adult, dim_education)
    adult = join_dim_marital_status(adult, dim_marital_status)
    adult = join_dim_n_country(adult, dim_native_country)
    adult = join_dim_occupation(adult, dim_occupation)
    adult = join_dim_race(adult, dim_race)
    adult = join_dim_relationship(adult, dim_relationship)
    adult = join_dim_sex(adult, dim_sex)
    adult = join_workclass(adult, dim_workclass)
    adult = join_dim_time(adult, dim_time)

    adult = adult.select(*schema_business)
    adult.printSchema()
    dao.save(adult, "bs_fact_adult", "append", "id_current_data")