import os
import argparse
import pickle
from json import dumps

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic


class TweetStreamAnalyzer():
    def __init__(self, upstreamTopic, posDict, negDict):
        self.upstreamTopic = upstreamTopic
        self.posDict = posDict
        self.negDict = negDict
        self.kafka_topic = "TweetStreamAnalyzer"
        self.kafka_broker = "localhost:9092"


    def run(self):
        try:
            admin = KafkaAdminClient(bootstrap_servers=self.kafka_broker)
            topic = NewTopic(name=self.kafka_topic,
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic])
        except Exception as e:
            print("Create Kafka topic failed: " + str(e))

        producer = KafkaProducer(bootstrap_servers=[self.kafka_broker],
                                 value_serializer=lambda x: dumps(x).encode('utf-8'))

        conf = SparkConf().setMaster("local[2]").setAppName("TweetStreamAnalyzer")
        sc = SparkContext(conf=conf)
        ssc = StreamingContext(sc, 10)

        kafkaStream = KafkaUtils.createDirectStream(
            ssc, topics=[self.upstreamTopic], kafkaParams={"metadata.broker.list": self.kafka_broker})
        tweets = kafkaStream.map(lambda x: x[1])
        words = tweets.flatMap(lambda line: line.split(" "))
        positive = words.map(lambda word: ('Positive', 1) if word in self.posDict else ('Positive', 0))
        negative = words.map(lambda word: ('Negative', 1) if word in self.negDict else ('Negative', 0))
        allSentiments = positive.union(negative)
        sentimentCounts = allSentiments.reduceByKey(lambda x, y: x + y)

        def handler(message):
            records = message.collect()
            for record in records:
                producer.send(self.kafka_topic, str(record))
                producer.flush()

        sentimentCounts.foreachRDD(handler)

        ssc.start()
        ssc.awaitTermination()
        ssc.stop(stopGraceFully=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="TweetStreamListener")

    args = parser.parse_args()

    LOCAL_ROOT = os.path.abspath("data") + os.sep
    with open(LOCAL_ROOT + 'sentiment_pos_dict.pickle', 'rb') as handle:
        posDict = pickle.load(handle)

    with open(LOCAL_ROOT + 'sentiment_neg_dict.pickle', 'rb') as handle:
        negDict = pickle.load(handle)

    tweetsStreamAnalyzer = TweetStreamAnalyzer(upstreamTopic=args.topic, posDict=posDict, negDict=negDict)
    tweetsStreamAnalyzer.run()


if __name__ == "__main__":
    main()