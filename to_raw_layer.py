# K.Nayan

import sys
import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
import logging
import re


class Setup:
    spark = SparkSession.builder.appName("Demo-Project-Stage-I").config('spark.ui.port', '4050').config("spark.master",
                                                                                                        "local").enableHiveSupport().getOrCreate()
    raw_df = spark.read.text("s3://mkc-tutorial-dest/tutorial/kafka-log-stream/299999.text")

    # raw_df = spark.read.text("C:\\Users\\Kamal Nayan\\Downloads\\log-increased.text")

    # spark = SparkSession.builder.appName("Demo Project").master("yarn").enableHiveSupport().getOrCreate()
    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_s3_sink(self):
        try:
            self.raw_df = self.spark.read.text("s3://mkc-tutorial-dest/tutorial/kafka-log-stream/299999.text")
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            self.raw_df.printSchema()
            # raw_df.show(10, truncate=False)


class Cleaning(Setup):
    def extract_columns_regex(self):
        regex_pattern = r'(.+?)\ - - \[(.+?)\] \"(.+?)\ (.+?)\ (.+?\/.+?)\" (.+?) (.+?) (.+?) \"(.+?)\"'

        self.raw_df = self.raw_df.withColumn("row_id", monotonically_increasing_id()) \
            .select("row_id", regexp_extract('value', regex_pattern, 1).alias('client/ip'),
                    regexp_extract('value', regex_pattern, 2).alias('datetime'),
                    regexp_extract('value', regex_pattern, 3).alias('method(GET)'),
                    regexp_extract('value', regex_pattern, 4).alias('request'),
                    regexp_extract('value', regex_pattern, 6).alias('status_code'),
                    regexp_extract('value', regex_pattern, 7).alias('size'),
                    regexp_extract('value', regex_pattern, 8).alias('referer'),
                    regexp_extract('value', regex_pattern, 9).alias('user_agent'))
        self.raw_df.show(truncate=False)

    def remove_special_character(self):
        # Remove any special characters in the request column(% ,- ? =)
        self.raw_df = self.raw_df.withColumn('request', regexp_replace('request', '%|-|\?=', ''))

    def size_to_kb(self):
        self.raw_df = self.raw_df.withColumn('size', round(self.raw_df.size / 1024, 2))
        return self.raw_df

    def remove_empty_string_with_null(self):
        self.raw_df = self.raw_df.select(
            [when(col(c) == "-", None).otherwise(col(c)).alias(c) for c in self.raw_df.columns])
        self.raw_df.show()
        return self.raw_df

    def count_null_each_row(self):
        return self.raw_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in self.raw_df.columns])

    def write_to_s3(self):
        self.raw_df.write.csv("s3a://project-layers-kamal/raw-layer/", mode="append",header=True)

    def write_to_hive(self):
        pass
        # **************************
        #self.raw_df.write.csv("s3a://project-layers-kamal/raw-layer/raw.csv", mode="append",header=True)
        self.raw_df.write.saveAsTable('RawDataTable')


if __name__ == "__main__":
    # Setup
    setup = Setup()
    try:
        setup.read_from_s3_sink()
    except Exception as e:
        logging.error('Error at %s', 'Reading from S3 Sink', exc_info=e)
        sys.exit(1)

    # Clean
    try:
        clean = Cleaning()
    except Exception as e:
        logging.error('Error at %s', 'Cleaning Object Creation', exc_info=e)
        sys.exit(1)
    try:
        clean.extract_columns_regex()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)

    try:
        clean.remove_special_character()
    except Exception as e:
        logging.error('Error at %s', 'remove_special_character', exc_info=e)
        sys.exit(1)

    try:
        clean.size_to_kb()
    except Exception as e:
        logging.error('Error at %s', 'size_to_kb', exc_info=e)
        sys.exit(1)

    try:
        clean.remove_empty_string_with_null()
    except Exception as e:
        logging.error('Error at %s', 'remove empty string with null', exc_info=e)
        sys.exit(1)

    try:
        clean.count_null_each_row()
    except Exception as e:
        logging.error('Error at %s', 'count null each column', exc_info=e)
        sys.exit(1)

    try:

        clean.write_to_s3()
        logging.info("Writing to Raw Layer S3 Successfull!")
    except Exception as e:
        logging.error('Error at %s', 'write_to_s3', exc_info=e)
        sys.exit(1)

    # try:
    #     clean.write_to_hive()
    # except Exception as e:
    #     logging.error('Error at %s', 'write to hive', exc_info=e)
