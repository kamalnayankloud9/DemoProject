# Author: K.Nayan
from time import sleep
from json import dumps
# from kafka import KafkaProducer

# read the file
import time
import random


class augment:
    # produce = KafkaProducer(bootstrap_servers=['b-2.s3sinkcluster.uj4gy1.c2.kafka.ap-south-1.amazonaws.com:9092','b-1.s3sinkcluster.uj4gy1.c2.kafka.ap-south-1.amazonaws.com:9092'],
    #                         value_serializer=lambda x: dumps(x).encode('utf-8'))
    # topic_name = 'demoproject'
    # input_filename = "/home/ec2-user/log_data_ip_request.txt"
    input_filename = "log_data_ip_request.txt"
    count = 0
    incrementBy = 0
    input_file_object = open(input_filename, 'r')

    file_object = open('IncreasedInput.txt', 'a')

    def changer(self):
        self.input_file_object.seek(0)
        for line_num, line in enumerate(self.input_file_object):
            last_digit = int(line.strip(" ").split(" ")[0].split(".")[-1])
            hour_digit = (line.split(" ")[3].split(":")[1])
            day_digit = int(line.split(" ")[3].split("/")[0][1:])
            if (line_num) < 300:
                # change the
                # temp_hour_changer = hour_digit + self.incrementBy
                temp_hour_changer = random.randint(10,23)

                # temp_digit_changer = last_digit + self.incrementBy
                temp_digit_changer = random.randint(1,255)
                # temp_day_changer = day_digit + self.incrementBy
                temp_day_changer = random.randint(1,31)
                # temp_digit_changer = last_digit + 1




                line = line.replace(str('.' + str(last_digit)), str('.' + str(temp_digit_changer)), 1)
                line = line.replace(str(str(hour_digit) + ':'), str(str(temp_hour_changer) + ':'), 1)
                line = line.replace(str('[' + str(day_digit) + '/'), str('[' + str(temp_day_changer) + '/'), 1)

            if (line_num < 50):
                line = line.replace("GET", "POST", 1)

            self.count += 1
            print("number of records inserted:--> " + str(self.count))
            # self.produce.send(self.topic_name, value=line)
            self.file_object.write(line)
        # if self.incrementBy < 1:
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
