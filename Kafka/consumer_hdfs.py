import os
from kafka import KafkaClient, SimpleConsumer
from datetime import datetime

kafka = KafkaClient("localhost:9092")

class HDFS_Consumer(object):
    '''
    This class is used to receive messages from Kafka, and save it to hdfs system.
    Messages are first saved to a temporary file on local machine, then transfered
    to hdfs atomically. Files are saved to a folder serving as steaming source 
    for Structured Streaming.
    '''
    def __init__(self, hdfs_directory, max_count):
        '''
        hdfs_directory is the folder where data is saved on hdfs.
        '''
        self.hdfs_dir = hdfs_directory
        self.count = 0
        self.max_count = max_count
        
    def getTimestamp(self):
        return datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    
    def consume_topic(self,topic,group,temp_dir):
        '''
        This function receive messages from Kafka then save it to a temporary
        first, then transfer the file to hdfs.
        '''
        # Create a kafka receiver to grap messages
        kafka_receiver = SimpleConsumer(kafka, group, topic, max_buffer_size=1310720000)
        
        self.timestamp = self.getTimestamp()        
        # Create a temp file to store messages
        self.temp_file_path = "%s/%s_%s.txt" % (temp_dir,self.timestamp,str(self.count))
        
        temp_file = open(self.temp_file_path, 'w')
        
        while self.count < self.max_count:
            # Get 100 messages each time
            messages = kafka_receiver.get_messages(count = 100, block = False)
            if not messages:
                continue
            
            # Write the messages to a file, one message per line
            for message in messages:
                temp_file.write(message.message.value + '\n')
            
            # For structured streaming, files need to be small at this point, set the size at 2 M
            if temp_file.tell() > 2000000:
                temp_file.close()
               
               # Copy the file to hdfs
                output_dir = "%s/%s" % (self.hdfs_dir, topic)
                os.system("hdfs dfs -mkdir %s" % output_dir)
                hdfs_path = "%s/%s_%s.txt" % (output_dir,self.timestamp,self.count)
                os.system("hdfs dfs -put -f %s %s" % (self.temp_file_path, hdfs_path))

                #remove the old file 
                os.remove(self.temp_file_path)

                #  Create a new temp file to store messages
                self.count +=1                
                self.timestamp = self.getTimestamp()
                self.temp_file_path = "%s/%s_%s.txt" % (temp_dir,self.timestamp,str(self.count))                
                temp_file = open(self.temp_file_path, 'w')

            # Inform zookeeper of position in the kafka queue
            kafka_receiver.commit()
        
        temp_file.close()
        
        
if __name__ == '__main__':
    
    hdfs_dir = "hdfs://ec2-52-45-70-95.compute-1.amazonaws.com:9000"
    group = "StructuredStreaming"
    topic = "sensor"
    temp_dir = "/home/ubuntu/data"
    max_count = 10
    
    hdfs_consumer = HDFS_Consumer(hdfs_dir, max_count)
    hdfs_consumer.consume_topic(topic,group,temp_dir)
