sudo yum install java-1.8.0

wget https://archive.apache.org/dist/kafka/2.2.1/kafka_2.12-2.2.1.tgz

tar -xzf kafka_2.12-2.2.1.tgz

echo "security.protocol=PLAINTEXT">client.properties
*********************


*********************
./bin/kafka-topics.sh --create --bootstrap-server b-2.s3sinkcluster.uj4gy1.c2.kafka.ap-south-1.amazonaws.com:9092,b-1.s3sinkcluster.uj4gy1.c2.kafka.ap-south-1.amazonaws.com:9092 --replication-factor 1 --partitions 1 --topic demoproject
**********************

b-1.mkccluster.xivdh0.c19.kafka.us-east-1.amazonaws.com:9092,b-2.mkccluster.xivdh0.c19.kafka.us-east-1.amazonaws.com:9092

***************************************
./bin/kafka-console-producer.sh --broker-list b-2.projectcluster.ksn0l9.c19.kafka.us-east-1.amazonaws.com:9092,b-1.projectcluster.ksn0l9.c19.kafka.us-east-1.amazonaws.com:9092 --producer.config client.properties --topic mkc-tutorial-topic
***************************************

***********************************************************************************
connector.class=io.lenses.streamreactor.connect.aws.s3.sink.S3SinkConnector
key.converter.schemas.enable=false
connect.s3.kcql=INSERT INTO mkc-tutorial-dest:tutorial SELECT * FROM mkc-tutorial-topic WITH_FLUSH_COUNT = 319
aws.region=us-east-1
tasks.max=2
topics=mkc-tutorial-topic
schema.enable=false
value.converter=org.apache.kafka.connect.storage.StringConverter
errors.log.enable=true
key.converter=org.apache.kafka.connect.storage.StringConverter
*********************************************************************************

connector.class=io.lenses.streamreactor.connect.aws.s3.sink.S3SinkConnector
key.converter.schemas.enable=false
connect.s3.kcql=INSERT INTO layer-s3-sink:IncreasedInput SELECT * FROM demoproject STOREAS 'Text' WITH_FLUSH_COUNT = 100000
aws.region=ap-south-1
tasks.max=2
topics=demoproject
schema.enable=false
value.converter=org.apache.kafka.connect.storage.StringConverter
errors.log.enable=true
key.converter=org.apache.kafka.connect.storage.StringConverter


*********************************************************************************


**************************
curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py
python get-pip.py
pip install kafka-python
****************************

****************************
>>vim producer.py

from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name = 'mkc-tutorial-topic'
producer = KafkaProducer(bootstrap_servers=['b-1.mkccluster.xivdh0.c19.kafka.us-east-1.amazonaws.com:9092','b-2.mkccluster.xivdh0.c19.kafka.us-east-1.amazonaws.com:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

for e in range(1000):
    data = {'number' : e}
    print(data)
    producer.send(topic_name,value=data)
    sleep(1)
	
>>python producer.py
******************************



******************************
>>vim consumer.py

from kafka import KafkaConsumer
import json
consumer = KafkaConsumer ('mkc-tutorial-topic',bootstrap_servers = ['b-2.projectcluster.ksn0l9.c19.kafka.us-east-1.amazonaws.com:9092','b-1.projectcluster.ksn0l9.c19.kafka.us-east-1.amazonaws.com:9092'],auto_offset_reset='earliest')
for message in consumer:
    print(message)

>>python consumer.py
******************************



**************************
connector.class=io.lenses.streamreactor.connect.aws.s3.sink.S3SinkConnector
key.converter.schemas.enable=false
connect.s3.kcql=INSERT INTO mkc-tutorial-dest:tutorial SELECT * FROM mkc-tutorial-topic STOREAS `Text` WITH_FLUSH_COUNT = 300000
aws.region=us-east-1
tasks.max=2
topics=mkc-tutorial-topic
schema.enable=false
value.converter=org.apache.kafka.connect.storage.StringConverter
errors.log.enable=true
key.converter=org.apache.kafka.connect.storage.StringConverter

***************************************************************

spark-submit --master yarn --deploy-mode client to_raw_layer.py



spark-submit --packages net.snowflake:snowflake-jdbc:3.11.1,net.snowflake:spark-snowflake_2.11:2.5.7-spark_2.4 --master yarn --deploy-mode client to_raw_layer.py
