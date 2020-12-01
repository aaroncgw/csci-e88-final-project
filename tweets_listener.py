import os
import json
from json import dumps
from http.client import IncompleteRead
import tweepy
import pykafka

import pandas as pd

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic


class TweetStreamListener(tweepy.StreamListener):
    def __init__(self):
        self.client = pykafka.KafkaClient("localhost:9092")
        self.kafka_broker = "localhost:9092"
        self.kafka_topic = "TweetStreamListener"

    def on_connect(self):
        try:
            self.producer = self.client.topics[bytes('TweetStreamListener', 'ascii')].get_producer()
            admin = KafkaAdminClient(bootstrap_servers=self.kafka_broker)
            topic = NewTopic(name=self.kafka_topic,
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic])
        except Exception as e:
            print("Create Kafka topic failed: " + str(e))

    def on_data(self, data):
        try:
            tweet = json.loads(data)

            if "extended_tweet" in tweet:
                tweet_text = tweet['extended_tweet']['full_text']
            else:
                tweet_text = tweet['text']

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




