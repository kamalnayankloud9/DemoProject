#K.Nayan
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
    spark = SparkSession.builder.appName("Demo-Project-Stage-II").config('spark.ui.port', '4050').config("spark.master","local").enableHiveSupport().getOrCreate()
    # clean_df = spark.read.csv("s3://project-layers-kamal/raw-layer/raw.csv/part-00000-c1ede5a7-dcd4-480c-94fa-2df9dbba99a1-c000.csv",header=True)
    # clean_df = spark.read.csv("C:\\Users\\Kamal Nayan\\Desktop\\Layers\\Raw\\raw.csv",header=True,inferSchema=True)
    clean_df = spark.read.csv("s3://layer-raw/raw_layer/", header=True, inferSchema=True)


    # spark = SparkSession.builder.appName("Demo Project").master("yarn").enableHiveSupport().getOrCreate()
    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_s3_raw(self):
        try:
            # self.clean_df = self.spark.read.csv("C:\\Users\\Kamal Nayan\\Desktop\\Layers\\Raw\\raw.csv",header=True,inferSchema=True)
            # self.clean_df = self.spark.read.csv("s3://project-layers-kamal/raw-layer/raw.csv/part-00000-c1ede5a7-dcd4-480c-94fa-2df9dbba99a1-c000.csv",header=True)
            self.clean_df = self.spark.read.csv("s3://layer-raw/raw_layer/", header=True, inferSchema=True)

        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            pass
            # self.clean_df.printSchema()
            # self.clean_df.show()
            # raw_df.show(10, truncate=False)

class CleanLayer(Setup):

    def drop_duplicate_values(self):
        self.clean_df = self.clean_df.drop_duplicates(["clientip", "datetime", "method"]).drop("row_id")
        self.clean_df = self.clean_df.withColumn("increasing_id", monotonically_increasing_id())
        window = Window.orderBy(col('increasing_id'))
        self.clean_df = self.clean_df.withColumn('row_id', row_number().over(window))

        #rearranging the order of columns
        self.clean_df = self.clean_df.select("row_id", "clientip", "datetime", "method", "request", "status_code", "size",
                                         "referer", "user_agent")

    #mm-dd-yyyy hh24:mi:ss
    def datetime_formatter(self):
        self.clean_df = self.clean_df.withColumn("datetime", split(self.clean_df["datetime"], ' ').getItem(0)).withColumn("datetime",to_timestamp("datetime",'dd/MMM/yyyy:HH:mm:ss'))
                                                                                                 # .withColumn("datetime",date_format("datetime",'MM-dd-yyyy:HH:mm:ss'))

        self.clean_df.show()


    def referer_present(self):
        self.clean_df = self.clean_df.withColumn("referer_present",
                                      when(col("referer")=="N/A", "N") \
                                      .otherwise("Y"))
        self.clean_df.show()

    def count_null_each_column(self):
         check_null_count= self.clean_df.select([count(when(col(c).isNull(), c)).alias(c) for c in self.clean_df.columns])
         # check_null_count.show()

    def write_to_s3(self):
        #self.clean_df.write.csv("s3a://pspark-submit --master yarn --deploy-mode client to_raw_layer.pyroject-layers-kamal/clean-layer/clean.csv", mode="append",header=True)
        # self.clean_df.printSchema()
        print("writing to clean s3")
        self.clean_df.write.csv("s3://layer-cleansed/cleansed/", mode="overwrite", header=True)

    def write_to_hive(self):
        # **************************
        print("writing to hive")
        sqlContext = HiveContext(self.spark.sparkContext)
        sqlContext.sql('DROP TABLE IF EXISTS cleanse_og_details')
        self.clean_df.write.saveAsTable('cleanse_og_details')

    def write_to_snowflake(self):
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
        snowflake_database = "kamaldb"
        snowflake_schema = "public"
        target_table_name = "curated_log_details"
        snowflake_options = {
            "sfUrl": "jn94146.ap-south-1.aws.snowflakecomputing.com",
            "sfUser": "*******",
            "sfPassword": "********",
            "sfDatabase": snowflake_database,
            "sfSchema": snowflake_schema,
            "sfWarehouse": "curated_snowflake"
        }
        spark = SparkSession.builder \
            .appName("Demo_Project").enableHiveSupport().getOrCreate()

        #clean
        #writing to snowflake
        df_clean_sn = spark.read \
            .format('csv').load('s3://layer-cleansed/cleansed/', header=True)
        df_clean_sn = df_clean_sn.select("*")
        df_clean_sn.write.format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "log_clean_details") \
            .option("header", "true") \
            .mode("overwrite") \
            .save()


if __name__ == "__main__":
    #SetUp
    setup = Setup()
    try:
        setup.read_from_s3_raw()
    except Exception as e:
        logging.error('Error at %s', 'Reading from S3 Sink', exc_info=e)
        sys.exit(1)


    #Clean


    try:
        cleanLayer = CleanLayer()
    except Exception as e:
        logging.error('Error at %s', 'Error at CleanLayer Object Creation', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.drop_duplicate_values()
    except Exception as e:
        logging.error('Error at %s', 'Error at Drop duplicates', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.datetime_formatter()
    except Exception as e:
        logging.error('Error at %s', 'Error at datetime formatter', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.referer_present()
    except Exception as e:
        logging.error('Error at %s', 'Error at referer present', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.count_null_each_column()
    except Exception as e:
        logging.error('Error at %s', 'count null each row', exc_info=e)
        sys.exit(1)


    try:
        #logging.INFO("Writing to S3 cleansed Layer......")
        cleanLayer.write_to_s3()
        #logging.INFO("Writing to Clean Layer S3 Successfully!")
    except Exception as e:
        logging.error('Error at %s', 'write_to_s3', exc_info=e)
        sys.exit(1)

    try:
        cleanLayer.write_to_hive()
    except Exception as e:
        logging.error('Error at %s', 'write to hive', exc_info=e)

    try:
        cleanLayer.write_to_snowflake()
    except Exception as e:
        logging.error('Error at %s', 'write to hive', exc_info=e)





