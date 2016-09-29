import sys
from datetime import datetime
from datetime import timedelta
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import json
import time
from random import randint

data_dir = '/home/ubuntu/data/subject'
src_filelist = ['101.dat','102.dat','103.dat','104.dat','105.dat','106.dat','107.dat','108.dat']

class Producer(object):

    def __init__(self, addr, group_id):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)
        self.group_id = group_id

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
                for num in range(1000):
                    user_id = "user_%s_%s" % (self.group_id,num) 
                    event_time = (start_time + timedelta(0,num_lines)).strftime('%Y-%m-%d %H:%M:%S')
                    hr = int(token[2]) + randint(0,4) - 2
                    msg = {'id':user_id, 'time':event_time,'hr':hr}
                    json_msg = json.dumps(msg)
                    print json_msg  
                    self.producer.send_messages('sensor',str(self.group_id),json_msg)
                line_inx += 1
                num_lines += 1
                line_inx = line_inx % max_inx
                time.sleep(2)

            line_inx += 1
            num_lines += 1
            line_inx = line_inx % max_inx
                
if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    group_id = int(args[2])
    producer = Producer(ip_addr,group_id)
    source_file = data_dir + src_filelist[producer.group_id-1]
    producer.produce_msgs(source_file)
