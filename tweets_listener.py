import os
import json
import argparse
import datetime
from json import dumps
from http.client import IncompleteRead
import tweepy
import re
import pykafka

import pandas as pd

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic


# def deEmojify(text):
#     regrex_pattern = re.compile(pattern = "["
#         u"\U0001F600-\U0001F64F"  # emoticons
#         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
#         u"\U0001F680-\U0001F6FF"  # transport & map symbols
#         u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
#                            "]+", flags = re.UNICODE)
#     return regrex_pattern.sub(r'',text)

class TweetStreamListener(tweepy.StreamListener):
    def __init__(self):
        self.client = pykafka.KafkaClient("localhost:9092")
        self.producer = self.client.topics[bytes('TweetStreamListener', 'ascii')].get_producer()

    # def on_connect(self):
    #     self.producer = KafkaProducer(bootstrap_servers=[self.kafka_broker],
    #                                   value_serializer=lambda x: dumps(x).encode('utf-8'))
    #
    #     try:
    #         admin = KafkaAdminClient(bootstrap_servers=self.kafka_broker)
    #         topic = NewTopic(name=self.kafka_topic,
    #                          num_partitions=1,
    #                          replication_factor=1)
    #         admin.create_topics([topic])
    #     except Exception as e:
    #         print("Create Kafka topic failed: " + str(e))

    #
    # def on_status(self, status):
    #     """ This method is called whenever new data arrives from live stream.
    #             We asynchronously push this data to kafka queue"""
    #     now = datetime.datetime.now()
    #     print(now.strftime("%Y-%m-%d %H:%M:%S"))
    #     print(status.text)
    #
    #     try:
    #         self.producer.send(self.kafka_topic, status.text)
    #     except Exception as e:
    #         print(e)
    #         return False
    #     return True

    def on_data(self, data):
        try:
            tweet = json.loads(data)

            if "extended_tweet" in tweet:
                tweet_text = tweet['extended_tweet']['full_text']
            else:
                tweet_text = tweet['text']

            # if tweet_text:
            #     data = {
            #         'created_at': tweet['created_at'],
            #         'tweet': tweet_text.replace(',', '')
            #     }

            if tweet_text:
                send_data = '{}'
                json_send_data = json.loads(send_data)
                json_send_data['tweet'] = tweet_text.replace(',', '')
                json_send_data['created_at'] = tweet['created_at']
                self.producer.produce(bytes(json.dumps(json_send_data),'ascii'))
                print("Published to Kafka: " + str(json_send_data))
        except Exception as e:
            print("Exception: " + str(e))
            print(data)

    def on_error(self, status_code):
        if (status_code == 420):
            return False  # 420 error occurs when rate limit exceeds
        print("Error received in TweetStreamListener: " + str(status_code))
        return True # Don't kill the stream

    def on_timeout(self):
        print("TweetStreamListener timeout!")
        return True # Don't kill the stream


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--topic", default="TweetStreamListener")
    #
    # args = parser.parse_args()

    LOCAL_ROOT = os.path.abspath("data") + os.sep
    df = pd.read_csv(LOCAL_ROOT + "twitter_users.csv")
    user_ids = df["id"].apply(str).to_list()

    auth = tweepy.OAuthHandler(os.environ["KEY"], os.environ["KEY_SECRET"])
    auth.set_access_token(os.environ["TOKEN"], os.environ["TOKEN_SECRET"])
    api = tweepy.API(auth)

    while True:
        try:
            tweetStreamListener = tweepy.Stream(auth=api.auth, listener=TweetStreamListener())
            #tweetStreamListener.filter(languages=['en'], track=['bitcoin'])
            tweetStreamListener.filter(follow=user_ids[:50], languages=['en'])
        except IncompleteRead:
            # Oh well, reconnect and keep trucking
            continue
        except KeyboardInterrupt:
            # Or however you want to exit this loop
            tweetStreamListener.disconnect()
            break


if __name__ == '__main__':
    main()




