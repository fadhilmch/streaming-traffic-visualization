import pykafka
import json
import tweepy
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
import credential

#TWITTER API AUTH
auth = OAuthHandler(credential.TWITTER_CONSUMER_KEY[0], credential.TWITTER_CONSUMER_SECRET[0])
auth.set_access_token(credential.TWITTER_TOKEN[0], credential.TWITTER_TOKEN_SECRET[0])

api = tweepy.API(auth)

#Twitter Stream Listener
class KafkaPushListener(StreamListener):          
    def __init__(self):
        self.client = pykafka.KafkaClient("localhost:9092")

        #Get Producer that has topic name is Twitter
        self.producer = self.client.topics[bytes("tweet1", "ascii")].get_producer()
  
    def on_data(self, data):
        #Producer produces data for consumer
        #Data comes from Twitter
        self.producer.produce(bytes(data, "ascii"))
        print(len(data))
        print(data)
        return True
                                                                                                                                           
    def on_error(self, status):
        print(status)
        return True
    
#Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

#Produce Data that has Game of Thrones hashtag (Tweets)
twitter_stream.filter(track=['jakarta'])