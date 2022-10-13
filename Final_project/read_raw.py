class read_raw:
    import regex as re
    def read_from_text(spark,df):
        base_df = spark.read.text("s3://demoprojectkamal/raw_data/log_data_ip_request.txt")
        base_df.printSchema()
        base_df.show(10, truncate=False)
        """**Counting number of Rows and Columns**"""
        print((base_df.count(), len(base_df.columns)))


        host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
        ts_pattern = r'\d{2}\/[a-zA-Z]{3,}\/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2}'
        method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
        content_size_pattern = r'(200)\s(\d{4,})'
        referer_pattern = r'("https\S+")'
        useragent_pattern = r'(Mozilla|Dalvik|Goog|torob|Bar).\d\S+\s\((\S\w+;?\s\S+(\s\d\.(\d\.)?\d)?)'

        logs_df = base_df.withColumn("id",monotonically_increasing_id())\
                         .select("id",regexp_extract('value', host_pattern, 1).alias('host'),
                                 regexp_extract('value', ts_pattern, 0).alias('timestamp'),
                                 regexp_extract('value',method_uri_protocol_pattern, 1).alias('method'),
                                 regexp_extract('value',method_uri_protocol_pattern, 2).alias('request'),
                                 regexp_extract('value',content_size_pattern, 1).alias('status_code'),
                                 regexp_extract('value',content_size_pattern, 2).alias('size'),
                                 regexp_extract('value',referer_pattern,1).alias('referer'),
                                 regexp_extract('value',useragent_pattern,0).alias('user_agent'))

        logs_df.show()
        return logs_df
