import os
from kafka import KafkaClient, SimpleConsumer
from datetime import datetime

kafka = KafkaClient("localhost:9092")
