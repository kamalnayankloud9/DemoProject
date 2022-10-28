# Author: K.Nayan
from time import sleep
from json import dumps
from kafka import KafkaProducer

# read the file
import time


class augment:
    produce = KafkaProducer(bootstrap_servers=['b-1.mkccluster.xivdh0.c19.kafka.us-east-1.amazonaws.com:9092',
                                               'b-2.mkccluster.xivdh0.c19.kafka.us-east-1.amazonaws.com:9092'],
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
    topic_name = 'mkc-tutorial-topic'
    input_filename = "/home/ec2-user/log_data_ip_request.txt"
    count = 0
    incrementBy = 0
    input_file_object = open(input_filename, 'r')

    # file_object = open('sample_v2.txt', 'a')

    def changer(self):
        self.input_file_object.seek(0)
        for line_num, line in enumerate(self.input_file_object):
            last_digit = int(line.strip(" ").split(" ")[0].split(".")[-1])
            hour_digit = int(line.split(" ")[3].split(":")[1])
            if (line_num) < 10:
                # change the
                temp_hour_changer = hour_digit + self.incrementBy
                temp_digit_changer = last_digit + self.incrementBy
                # temp_digit_changer = last_digit + 1
                line = line.replace(str('.' + str(last_digit)), str('.' + str(temp_digit_changer)), 1)
                line = line.replace(str(str(hour_digit) + ':'), str(str(temp_hour_changer) + ':'), 1)

                # if (line_num + 1) % 9 == 0:
                if (self.incrementBy >= 1):
                    line = line.replace("GET", "POST", 1)

            self.count += 1
            # print("number of records inserted:--> " + str(self.count))
            self.produce.send(self.topic_name, value=line)
            # self.file_object.write(line)
        if self.incrementBy < 1:
            self.incrementBy += 1

    def closer(self):
        self.input_file_object.close()
        self.file_object.close()
        print("file closed")


if __name__ == "__main__":
    # Number of records wanted in Lakhs

    Num_of_records_wanted_in_lakhs = int(input("Enter the number of records wanted(in Lakhs): "))
    # Number of times inp file to be accessed
    num_access = int((Num_of_records_wanted_in_lakhs * 100000) / 319)
    print(num_access)

    augment = augment()
    for i in range(num_access):
        print("reading for " + str(i + 1) + " time")
        # time.sleep(1)
        augment.changer()

    augment.closer()
