class clean_to_curated:

    def clean_to_curated_funct(df, spark):

        """**Curated Layer**"""

        curated = clean_df.drop("referer")

        curated.show()
        try:
            curated.write.csv("s3://demoprojectkamal/curatedlayer/curated.csv",mode="overwrite")
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)

        """**saving curated data to HIVE**"""

        curated.write.mode("overwrite").saveAsTable("curated")

        return curated



