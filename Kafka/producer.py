import sys
from datetime import datetime
from datetime import timedelta
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import json
import time

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_file):
        with open(source_file,'r') as f:
            lines = f.readlines()
        start_time = datetime.now()
        num_lines = 0
        line_inx = 0
        max_inx = len(lines)

        while line_inx < max_inx:
            token = lines[line_inx].strip().split()

            line_inx = line_inx % max_inx

            if token[2] != 'NaN':
                msg = {'id':'user_test', 'time': (start_time + timedelta(0,num_lines)).strftime('%Y-%m-%d %H:%M:%S'),'hr':int(token[2])}
                json_msg = json.dumps(msg)
                print json_msg
                self.producer.send_messages('test3',str(hash(msg['time'])),json_msg)
                line_inx += 1
                num_lines += 1
                line_inx = line_inx % max_inx
                time.sleep(1)

            line_inx += 1
            num_lines += 1
            line_inx = line_inx % max_inx

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    source_file = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(source_file)
