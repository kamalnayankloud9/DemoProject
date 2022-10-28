#K.Nayan
import sys
import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import logging
import re

class Setup:
    spark = SparkSession.builder.appName("Demo-Project-Stage-II").config('spark.ui.port', '4050').config("spark.master","local").enableHiveSupport().getOrCreate()
    clean_df = spark.read.csv("s3://project-layers-kamal/raw-layer/raw.csv/part-00000-c1ede5a7-dcd4-480c-94fa-2df9dbba99a1-c000.csv",header=True)
    #clean_df = spark.read.csv("C:\\Users\\Kamal Nayan\\Desktop\\Layers\\Raw\\raw.csv",header=True)


    # spark = SparkSession.builder.appName("Demo Project").master("yarn").enableHiveSupport().getOrCreate()
    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_s3_raw(self):
        try:
            # self.clean_df = self.spark.read.text("s3://project-layers-kamal/raw-layer/raw.csv")
            self.clean_df = self.spark.read.csv("s3://project-layers-kamal/raw-layer/raw.csv/part-00000-c1ede5a7-dcd4-480c-94fa-2df9dbba99a1-c000.csv",header=True)
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            self.clean_df.printSchema()
            self.clean_df.show()
            # raw_df.show(10, truncate=False)

class CleanLayer(Setup):
    #mm-dd-yyyy hh24:mi:ss
    def datetime_formatter(self):
        self.clean_df = self.clean_df.withColumn("datetime", split(self.clean_df["datetime"], ' ').getItem(0)).withColumn("datetime",to_timestamp("datetime",'dd/MMM/yyyy:hh:mm:ss'))\
                                                                                                                .withColumn("datetime",to_timestamp("datetime",'MMM/dd/yyyy:hh:mm:ss'))
        # self.clean_df.show()

    def referer_present(self):
        self.clean_df = self.clean_df.withColumn("referer_present(Y/N)",
                                      when(col("referer") == None, "N") \
                                      .otherwise("Y"))
        self.clean_df.show()

    def write_to_s3(self):
        self.clean_df.write.csv("s3a://project-layers-kamal/clean-layer/clean.csv", mode="append",header=True)

    def write_to_hive(self):
        pass
        # **************************
        self.raw_df.write.saveAsTable('cleantable')


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

        clean.write_to_s3()
        logging.info("Writing to Clean Layer S3 Successfull!")
    except Exception as e:
        logging.error('Error at %s', 'write_to_s3', exc_info=e)
        sys.exit(1)

    # try:
    #     clean.write_to_hive()
    # except Exception as e:
    #     logging.error('Error at %s', 'write to hive', exc_info=e)



