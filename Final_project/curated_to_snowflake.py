class curated_to_snowflake:

    def curated_to_snowflake_funct(df, spark):
            SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
            snowflake_database = "kamal_db"
            snowflake_schema = "PUBLIC"
            # source_table_name = "AGENTS"
            snowflake_options = {
                "sfUrl": "sg21349.ap-south-1.aws.snowflakecomputing.com",
                "sfUser": "KAMALNAYAN",
                "sfPassword": "Atvks123@@",
                "sfDatabase": snowflake_database,
                "sfSchema": snowflake_schema,
                "sfWarehouse": "DEMO_WH"
            }
            df = spark.read \
                .format('CSV').load(
                's3://demoprojectkamal/curatedlayer/curated.csv')
            df1 = df.select("id")
            df1.write.format("snowflake") \
                .options(**snowflake_options) \
                .option("dbtable", "curatedcode").mode("overwrite") \
                .save()








