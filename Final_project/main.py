import regex as re
from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from curated_to_snowflake import *
from write_to_clean import *
from clean_to_curated import *
from curated_to_agg import *
from read_raw import read_from_text
from write_to_clean import *

"""**Creating Spark Object with HIVE Support**"""
spark = SparkSession.builder.appName("Demo Project").master("yarn").enableHiveSupport().getOrCreate()

sc=spark.sparkContext

def __main__():


    #function reads the input data from s3 and returns the dataframe
    df = read_from_text(sc,spark) #done

    #fucntion cleans the data and puts it into cleansed layer in s3 bucket
    # and returns the cleaned dataframe
    df_clean = write_to_clean(df,spark,sc) #

    #fucntion transforms the data and writes to curated layer along with curated layer
    df_curated = clean_to_curated(df_clean,spark) #

    #function moves the data from curated to aggregated layer
    curated_to_agg(df_curated,df,spark,sc)

    #moves the data from curated to snowflake
    curated_to_snowflake_funct(df, spark)













