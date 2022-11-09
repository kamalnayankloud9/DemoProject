# K.Nayan
import sys
import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import logging
import re


class Setup:
    spark = SparkSession.builder.appName("Demo-Project-Stage-III").config('spark.ui.port', '4050').config(
        "spark.master", "local").enableHiveSupport().getOrCreate()
    # curated_df = spark.read.csv(
    #     "s3://project-layers-kamal/clean-layer/clean.csv/part-00000-65b1e49f-2d58-479a-93b3-0e58cf8450b2-c000.csv",
    #     header=True, inferSchema=True)

    # curated_df = spark.read.csv(
    #     "C:\\Users\Kamal Nayan\Downloads\part-00000-4f20da17-b537-456b-9269-5a1d4ca9ac77-c000.csv", header=True,
    #     inferSchema=True)

    curated_df = spark.read.csv(
        "s3://layer-cleansed/cleansed/", header=True,inferSchema=True)

    # spark = SparkSession.builder.appName("Demo Project").master("yarn").enableHiveSupport().getOrCreate()
    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_s3_clean(self):
        try:
            # self.curated_df = self.spark.read.csv(
            #     "s3://project-layers-kamal/clean-layer/clean.csv/part-00000-65b1e49f-2d58-479a-93b3-0e58cf8450b2-c000.csv",
            #     header=True, inferSchema=True)
            self.curated_df = self.spark.read.csv("s3://layer-cleansed/cleansed/",header=True,inferSchema=True)
            # self.curated_df = self.spark.read.csv(
            #     "C:\\Users\Kamal Nayan\Downloads\part-00000-4f20da17-b537-456b-9269-5a1d4ca9ac77-c000.csv", header=True,
            #     inferSchema=True)
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            self.curated_df.printSchema()
            # self.curated_df.show()
            # raw_df.show(10, truncate=False)


class Curated(Setup):

    def drop_referer(self):
        self.curated_df = self.curated_df.drop("referer")
        self.curated_df.show()

    def write_to_s3(self):
        self.curated_df.write.csv("s3://layer-curated/curated/", mode="overwrite", header=True)

    def write_to_hive(self):
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS curate_og_details')
        self.curated_df.write.saveAsTable("curate_og_details")



class Agg(Curated):


    # check distinct no. of user device ie ip
    def check_distinct_user(self):
        self.curated_df.select("clientip").distinct().count()

    # add column hour,Get,Post,Head
    def add_temp_columns(self):
        df_temp = self.curated_df.withColumn("No_get", when(col("method") == "GET", "GET")) \
            .withColumn("No_post", when(col("method") == "POST", "POST")) \
            .withColumn("No_Head", when(col("method") == "HEAD", "HEAD")) \
            .withColumn("day", to_date(col("datetime"))) \
            .withColumn("hour", hour(col("datetime"))) \
            .withColumn("day_hour", concat(col("day"), lit(" "), col("hour")))

        check_null_count= df_temp.select([count(when(col(c).isNull(), c)).alias(c) for c in df_temp.columns])
        check_null_count.show()

        df_temp.show()
        return df_temp

    # perform aggregation per device
    def agg_per_device(self, df_temp):
        df_agg_per_device = df_temp.select("row_id", "day_hour", "clientip", "no_get", "no_post", "no_head") \
            .groupBy("day_hour", "clientip") \
            .agg(count("row_id").alias("count_row_id"),
                 count(col("No_get")).alias("no_get"),
                 count(col("No_post")).alias("no_post"),
                 count(col("No_head")).alias("no_head")) \
            # .orderBy(col("row_id").desc())

        df_agg_per_device.show()
        return df_agg_per_device

    # perform aggregation across device
    def agg_across_device(self,df_temp):
        df_agg_across_device = df_temp.select("*") \
            .groupBy("day_hour") \
            .agg(
                count("clientip").alias("no_of_clients"),
                count("row_id").alias("count_row_id"),
                count(col("No_get")).alias("no_get"),
                count(col("No_post")).alias("no_post"),
                count(col("No_head")).alias("no_head")
                )
        df_agg_across_device.show()
        return df_agg_across_device

    # write to s3 curated-layer-aggregations
    def write_to_s3_agg(self,df_agg_per_device,df_agg_across_device):
        df_agg_per_device.write.csv("s3://layer-curated/aggregation-per-device/", header=True,
                                    mode='overwrite')
        df_agg_across_device.write.csv("s3://layer-curated/aggregation-across-device/",
                                       header=True, mode='overwrite')

    # write to hive
    def write_to_hive(self,df_agg_per_device,df_agg_across_device):
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS log_agg_per_device')
        df_agg_per_device.write.saveAsTable('log_agg_per_device')
        sqlContext.sql('DROP TABLE IF EXISTS log_agg_across_device')
        df_agg_across_device.write.saveAsTable('log_agg_across_device')

    def write_to_snowflake(self):
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        snowflake_database = "kamaldb"
        snowflake_schema = "public"
        target_table_name = "curated_log_details"
        snowflake_options = {
            "sfUrl": "jn94146.ap-south-1.aws.snowflakecomputing.com",
            "sfUser": "***",
            "sfPassword": "*****",
            "sfDatabase": snowflake_database,
            "sfSchema": snowflake_schema,
            "sfWarehouse": "curated_snowflake"
        }
        spark = SparkSession.builder \
            .appName("Demo_Project").enableHiveSupport().getOrCreate()

        #curated
        df_curated_sn = spark.read \
            .format('csv').load('s3://layer-curated/curated/', header=True)
        df_curated_sn = df_curated_sn.select("*")
        df_curated_sn.write.format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "log_curated_details") \
            .option("header", "true") \
            .mode("overwrite") \
            .save()

        #Snowflake log_per_device
        df_per_device_sn = spark.read \
            .format('csv').load('s3://layer-curated/aggregation-per-device/', header=True)
        df_per_device_sn = df_per_device_sn.select("*")
        df_per_device_sn.write.format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "log_per_device") \
            .option("header", "true") \
            .mode("overwrite") \
            .save()

        #Snowflake log_across_device
        df_across_device_sn = spark.read \
            .format('csv').load('s3://layer-curated/aggregation-across-device/', header=True)
        df_across_device_sn = df_across_device_sn.select("*")
        df_across_device_sn.write.format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "log_across_device") \
            .option("header", "true") \
            .mode("overwrite") \
            .save()





if __name__ == '__main__':
    try:
        setup = Setup()
    except Exception as e:
        logging.error('Error at %s', 'Setup Object creation', exc_info=e)
        sys.exit(1)

    try:
        setup.read_from_s3_clean()
    except Exception as e:
        logging.error('Error at %s', 'read from s3 clean', exc_info=e)
        sys.exit(1)

    try:
        curated = Curated()
    except Exception as e:
        logging.error('Error at %s', 'curated Object creation', exc_info=e)
        sys.exit(1)

    try:
        curated.drop_referer()
    except Exception as e:
        logging.error('Error at %s', 'drop referer', exc_info=e)
        sys.exit(1)

    try:
        curated.write_to_s3()
    except Exception as e:
        logging.error('Error at %s', 'write to s3', exc_info=e)
        sys.exit(1)
    
    try:
        curated.write_to_hive()
    except Exception as e:
        logging.error('Error at %s', 'write to hive', exc_info=e)
        sys.exit(1)

    # Agg
    try:
        agg = Agg()
    except Exception as e:
        logging.error('Error at %s', 'error creating Object agg', exc_info=e)
        sys.exit(1)

    try:
        agg.check_distinct_user()
    except Exception as e:
        logging.error('Error at %s', 'check distinct user', exc_info=e)
        sys.exit(1)

    try:
        df_temp = agg.add_temp_columns()
    except Exception as e:
        logging.error('Error at %s', 'add_temp_columns', exc_info=e)
        sys.exit(1)

    try:
        df_temp_agg_per_device = agg.agg_per_device(df_temp)
    except Exception as e:
        logging.error('Error at %s', 'agg_per_device', exc_info=e)
        sys.exit(1)

    try:
        df_temp_agg_across_device= agg.agg_across_device(df_temp)
    except Exception as e:
        logging.error('Error at %s', 'agg_per_device', exc_info=e)
        sys.exit(1)

    try:
        agg.write_to_s3_agg(df_temp_agg_per_device,df_temp_agg_across_device)
    except Exception as e:
        logging.error('Error at %s', 'write to s3_agg', exc_info=e)
        sys.exit(1)

    try:
        agg.write_to_hive(df_temp_agg_per_device,df_temp_agg_across_device)
    except Exception as e:
        logging.error('Error at %s', 'write to s3_agg', exc_info=e)
        sys.exit(1)

    try:
        agg.write_to_snowflake()
    except Exception as e:
        logging.error('Error at %s', 'write to s3_snowflake', exc_info=e)
        sys.exit(1)




