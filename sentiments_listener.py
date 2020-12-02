import os
import pykafka
from pykafka.common import OffsetType
from elasticsearch import Elasticsearch
from datetime import datetime

def main():
    es = Elasticsearch('https://db2cb7cbe8834bb1a48f960a437f461d.us-east-1.aws.found.io:9243',
                    http_auth=(os.environ["ELASTIC_USERNAME"], os.environ["ELASTIC_PASSWORD"]))

    client = pykafka.KafkaClient("localhost:9092")
    topic = client.topics[bytes("TweetStreamSentiments", 'ascii')]
    consumer = topic.get_simple_consumer(consumer_group="mygroup",
                                        auto_offset_reset=OffsetType.LATEST,
                                        reset_offset_on_start=True)
    for msg in consumer:
        try:
            timestamp = datetime.strptime(msg.partition_key.decode().split(',')[1][1:-1], '%Y-%m-%d %H:%M:%S')
            eventTime = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
            json_send_data = {"timestamp" : eventTime,
                              "sentiment_score" : float(msg.value.decode())}

            print(json_send_data)
            es.index(index="tweets_sentiment_ts", id=msg.partition_key.decode(), body=json_send_data)
        except Exception as e:
            print("Exception: " + str(e))


if __name__ == "__main__":
    main()


