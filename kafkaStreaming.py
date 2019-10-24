from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os

if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 pyspark-shell'
    
    # Create Streaming Context and set batch interval
    conf = SparkConf().setMaster("local[*]").setAppName("twitter-sentiment")
    sc = SparkContext.getOrCreate(conf = conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("./checkpoints")
    ssc = StreamingContext(sc, 5)
    
#     lines = kafkaStream.map(lambda x: x[1])
#     lines.pprint()
#     ssc.start()
#     ssc.awaitTermination()
    
#     lines = kvs.map(lambda x: x[1])
#     counts = lines.flatMap(lambda line: line.split(" ")) \
#                   .map(lambda word: (word, 1)) \
#                   .reduceByKey(lambda a, b: a+b)
#     counts.pprint()
#     ssc.start()
#     ssc.awaitTermination()