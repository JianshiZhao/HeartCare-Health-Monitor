# HeartCare: Health Monitor

## Project Overview
Internet of things (IoTs) have been part of our lives nowadays. Lots of health monitor sensors are out there to measure the health charactors of our bodies. Those devices procudce timestamped data at various sampling rate. Effectivly and efficiently using of these data can potentially help people imporve healthcare quality, detect health anormalies, and remote servies. This project is to build a near real-time sensor signal processing platform, to deal with streaming of health sensor data. It has potential application in hospital, nursing home, etc. 

## Data Source
Data is generated through a Kafka producer, which simulates 8 data sources with total of 8000 users, with a rate of 1000 /s . The basic message is in json format:
{"id":"user_id","timestamp":"event_time", "hr": heart rate number}

For example:

{"hr": 117, "id": "Susan Nathan", "time": "2016-09-23 16:11:09"}

{"hr": 103, "id": "Anthony Laura", "time": "2016-09-21 14:01:25"}

{"hr": 133, "id": "Matthew Maria", "time": "2016-09-23 16:58:55"}

{"hr": 131, "id": "Jason Rachel", "time": "2016-09-22 16:33:45"}

{"hr": 101, "id": "Susan Nathan", "time": "2016-09-20 19:12:57"}



## Spark Streaming Pipline
- __Ingestion__


<img src = "https://github.com/JianshiZhao/HeartCare-Health-Monitor/blob/master/images/sparkstreamingpipe.png" alt = "Spark Streaming Pipline" width="800" >

Spark streaming divide the incoming data into mini-batches, each contains messages accumulated in a time window. The time window based on the time messages are received, not event time.
<img src = "https://github.com/JianshiZhao/HeartCare-Health-Monitor/blob/master/images/sparkstreamingscheme.png" alt = "Spark Streaming" width = "800">

> One challenge using system time is that messages may come out of order, which casuses inconsistancy and error in the results. 



## Spark Structured Streaming 
To overcome these problems, I experimented a new high-level API for streaming, Structured Streaming, added in Spark 2.0.0 (released on Jul 26 2016). Structured Streaming treats all arrived data as a unbounded input table. A new message is treated as a new row in the table. Streaming computation is like standard batch-like query on a static table and Spark runs it as an incremental query on the unbounded input table.


<img  src = "https://github.com/JianshiZhao/HeartCare-Health-Monitor/blob/master/images/ss1.png" alt = "Input Table", width = "400"> |<img  src = "https://github.com/JianshiZhao/HeartCare-Health-Monitor/blob/master/images/ss2.png" alt = "Structured Streaming" width = "400">

The advantages of this streaming model is that it garantees the consistancy of the result from the input table, and take acount of the out-of-order data by runing query over the whole input table. 


## Structured Streaming Pipeline
The following shows the pipeline for structured streaming. Ideally, we just need to switch the Spark Streaming with Structured Streaming. However, the first version of Structured Streaming supports limited data sources, only file source and socket source. There is no directed connector to Kafka (which will be added in the future release). Moreover, there isn't a direct way to save output results to Cassandra database.   

To walk around these obstacles, messages are grabed by a Kafka consumer and saved to folder on HDFS, which serves as a source folder for Structured Streaming. Then, after processing the data, results are write to a result table in memory, using the memory sink optition. This table can be written into Cassandra after all the data been processed.

<img src = "https://github.com/JianshiZhao/HeartCare-Health-Monitor/blob/master/images/structuredstreamingpipe.png" alt = "Structured Streaming Pipeline" width = "800">

One main feauture in Structured Streaming is that aggregations over a sliding event-time window is straitforward. Window-based aggregations are very similar to grouped aggregations, where aggregate values are maintained for each window the event-time of a row falls into. This make the average over event time much easier then Spark Streaming. 


## Demo Presentations
Presentation slides about this project is available <a href = "https://docs.google.com/presentation/d/1flMn2waduRLvoU9rn_o9kT73yvg4pUzHyn-i1D9i91s/edit?usp=sharing"> here </a>





