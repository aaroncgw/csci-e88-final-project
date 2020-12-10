import os
import json
from dateutil import parser
from http.client import IncompleteRead
import tweepy
import pykafka
import pandas as pd
from elasticsearch import Elasticsearch


class TweetStreamListener(tweepy.StreamListener):
    def __init__(self):
        self.client = pykafka.KafkaClient("localhost:9092")
        self.kafka_topic = "TweetStreamListener"
        self.producer = self.client.topics[bytes(self.kafka_topic, 'ascii')].get_producer()

        self.es = Elasticsearch('https://db2cb7cbe8834bb1a48f960a437f461d.us-east-1.aws.found.io:9243',
                                http_auth=(os.environ["ELASTIC_USERNAME"], os.environ["ELASTIC_PASSWORD"]))
        print("Connecting...")

    def on_connect(self):
        print("Connected!")

    def on_data(self, data):
        try:
            tweet = json.loads(data)
            tweet['timestamp'] = parser.parse(tweet['created_at']).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
            self.es.index(index=self.kafka_topic.lower(), body=tweet) #send raw data to elasticsearch directly

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
    df = pd.read_csv(LOCAL_ROOT + "twitter_userid.csv")
    user_ids = df["id"].apply(str).to_list() #tweepy could only filter tweets by user ids rather than usernames

    auth = tweepy.OAuthHandler(os.environ["KEY"], os.environ["KEY_SECRET"])
    auth.set_access_token(os.environ["TOKEN"], os.environ["TOKEN_SECRET"])
    api = tweepy.API(auth)

    while True:
        try:
            tweetStreamListener = tweepy.Stream(auth=api.auth, listener=TweetStreamListener())
            #tweetStreamListener.filter(languages=['en'], track=['bitcoin'])  #fitler by keywords
            tweetStreamListener.filter(follow=user_ids[:50], languages=['en'])  #fitler by user ids
        except IncompleteRead:
            # Oh well, reconnect and keep trucking
            continue
        except KeyboardInterrupt:
            # Or however you want to exit this loop
            tweetStreamListener.disconnect()
            break


if __name__ == '__main__':
    main()




