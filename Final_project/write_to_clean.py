class write_to_clean:


    """**Cleansed Layer**"""
    def write_to_clean_funct(df,spark):


        clean_df=logs_df.withColumn("referer_presentYN",
                            when(col("referer")=='' ,"N")\
                            .otherwise("Y"))
        clean_df.show()
        """**loading Cleansed data to S3**"""
        try:
            clean_df.write.csv("s3://demoprojectkamal/cleansedlayer/cleansed.csv",mode="overwrite")
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)

""""""
"""**Curated Layer**"""

curated=clean_df.drop("referer")

curated.show()

"""**loading curated data to S3**"""

curated.write.csv("s3://demoprojectkamal/curatedlayer/curated.csv",mode="overwrite")

"""**saving curated data to HIVE**"""

curated.write.mode("overwrite").saveAsTable("curated1")

"""**Converted datatype of Date**"""

from time import *
curated1=curated.withColumn("timestamp",to_timestamp("timestamp",'dd/MMM/yyyy:HH:mm:ss'))

curated1.printSchema()

"""**Extracting hour from date Column for Aggregation Tables**"""

df = curated1.withColumn("hour", hour(col("timestamp")))

user_device_pattern=r'(Mozilla|Dalvik|Goog|torob|Bar).\d\S+\s\((\S\w+;?\s\S+(\s\d\.(\d\.)?\d)?)'

logs_df_agg = base_df.withColumn("id",monotonically_increasing_id())\
                 .select("id",regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 0).alias('timestamp'),
                         regexp_extract('value',method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value',method_uri_protocol_pattern, 2).alias('request'),
                         regexp_extract('value',content_size_pattern, 1).alias('status_code'),
                         regexp_extract('value',content_size_pattern, 2).alias('size'),
                         regexp_extract('value',referer_pattern,1).alias('referer'),
                         regexp_extract('value',useragent_pattern,0).alias('user_agent'),
                         regexp_extract('value',user_device_pattern,2).alias('user_device'))

logs_df_agg.show()

"""**COunting total Devices**"""

logs_df_agg.select("user_device").distinct().count()

"""**Replacing blank records of Referer Column with Null**"""

from pyspark.sql.functions import col,when
df2=logs_df_agg.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in logs_df_agg.columns])
df2.show()

df2.filter(df2.referer.isNull()).count()

"""**Adding Hour Column in the Table for Aggregation**"""

from time import *
df_hours=df2.withColumn("timestamp",to_timestamp("timestamp",'dd/MMM/yyyy:HH:mm:ss'))

df_hour = df_hours.withColumn("hour", hour(col("timestamp")))

df_hours_getposthead = df_hour.withColumn("No_get", when(col("method")=="GET", "GET"))\
                                .withColumn("No_post" ,when(col("method")=="POST", "POST"))\
                                .withColumn("No_Head" ,when(col("method")=="HEAD", "HEAD"))\
                                .withColumn("hour", hour(col("timestamp")))

"""**Displaying Which Device have Get,post,head method **"""

df_hours_getposthead.show()

df_hour.select("*").groupBy("user_device").sum("hour").show()

"""**Logs Per Device Table**"""

df_hours_getposthead_new = df_hours_getposthead.select("*").groupBy("user_device").agg(count("id").alias("row_id"),sum("hour").alias("day_hour"),count("host").alias("client/IP"),count(col("No_post")).alias("No_post"),count(col("No_get")).alias("No_get"),count(col("No_head")).alias("no_head"))

df_hours_getposthead_new.show()

"""**Logs Per Device Table loading in S3**"""

df_hours_getposthead_new.write.csv("s3://demoprojectkamal/aggregation/perdevice/perdevice.csv",mode="overwrite")

df_hours_getposthead_new.write.mode("overwrite").saveAsTable("logsperdevice")

"""**Logs Across The Devices**"""

df_across=df_hours_getposthead.select("*").agg(count("id").alias("row_id"),first("hour").alias("day_hour"),count("host").alias("client/IP"),count(col("No_post")).alias("No_post"),count(col("No_get")).alias("No_get"),count(col("No_head")).alias("no_head"))

df_across.show()

"""**Logs Across The Devices loading in S3**"""

df_across.write.csv("s3://sushant1010/Demoproject/aggregation/acrossdevice/acrossdevice.csv",mode="overwrite")
df_across.write.mode("overwrite").saveAsTable("logsacrossdevice")

"""**Storing tables into Snowflake**"""
"""
def main():
    
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database="curated"
    snowflake_schema="public"
    target_table_name="curatedtbl"
    snowflake_options = {
        "sfUrl": "jn94146.ap-south-1.aws.snowflakecomputing.com",
        "sfUser": "sushantsangle",
        "sfPassword": "Stanford@01",
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": "curated_snowflake"
    }
    
    curated.write.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "curated").mode("overwrite") \
        .save()
    
    df_across.write.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "df_perdevice").mode("overwrite") \
        .save()
    
    df_hours_getposthead_new.write.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "df_across").mode("overwrite") \
        .save()


main()
"""
