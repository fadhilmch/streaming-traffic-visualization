from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import sys
import json

if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 pyspark-shell'
    
    # Create Streaming Context and set batch interval
    conf = SparkConf().setMaster("local[*]").setAppName("twitter-sentiment")
    sc = SparkContext.getOrCreate(conf = conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("./checkpoints")
    ssc = StreamingContext(sc, 5)

    brokers = "localhost:9092"
    topic = [sys.argv[1]]
    kafkaParams = {"metadata.broker.list": "localhost:9092",
           "zookeeper.connect": "localhost:2181",
           "group.id": "kafka-spark-streaming",
           "zookeeper.connection.timeout.ms": "1000"}
    kafkaStream = KafkaUtils.createDirectStream(ssc, topic, kafkaParams)
    tweet = kafkaStream.map(lambda value: json.loads(value[1])). \
        map(lambda json_object: (json_object["user"]["screen_name"], json_object["text"]))
    tweet.pprint()
    ssc.start()
    ssc.awaitTermination()
    
    
    
    
    
#     from pyspark import SparkContext, SparkConf
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils
# import os
# import sys

# if __name__ == "__main__":
#     os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 pyspark-shell'
    

    


#     kafkaStream = KafkaUtils.createDirectStream(ssc, topic, kafkaParams)
    
#     raw = kafkaStream.flatMap(lambda kafkaS: [kafkaS])
#     print(raw)
#     print(type(raw))
    
# #     lines = kafkaStream.map(lambda x: x[1])
# #     lines.pprint()
# #     ssc.start()
# #     ssc.awaitTermination()
    
# #     lines = kvs.map(lambda x: x[1])
# #     counts = lines.flatMap(lambda line: line.split(" ")) \
# #                   .map(lambda word: (word, 1)) \
# #                   .reduceByKey(lambda a, b: a+b)
# #     counts.pprint()
#     ssc.start()
#     ssc.awaitTermination()