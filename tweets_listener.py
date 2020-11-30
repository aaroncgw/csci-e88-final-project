import os
import argparse
import datetime
from json import dumps
from http.client import IncompleteRead
import tweepy

import pandas as pd

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

class TweetStreamListener(tweepy.StreamListener):
    def __init__(self, tweepy_api):
        super(tweepy.StreamListener, self).__init__()
        self.api = tweepy_api
        self.kafka_topic = "TweetStreamListener"
        self.kafka_broker = "localhost:9092"

    def on_connect(self):
        self.producer = KafkaProducer(bootstrap_servers=[self.kafka_broker],
                                      value_serializer=lambda x: dumps(x).encode('utf-8'))

        try:
            admin = KafkaAdminClient(bootstrap_servers=self.kafka_broker)
            topic = NewTopic(name=self.kafka_topic,
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic])
        except Exception as e:
            print("Create Kafka topic failed: " + str(e))


    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
                We asynchronously push this data to kafka queue"""
        now = datetime.datetime.now()
        print(now.strftime("%Y-%m-%d %H:%M:%S"))
        print(status.text)

        try:
            self.producer.send(self.kafka_topic, status.text)
        except Exception as e:
            print(e)
            return False
        return True

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
            tweetStreamListener = tweepy.Stream(auth=api.auth, listener=TweetStreamListener(tweepy_api=api))
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




