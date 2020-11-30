import os
import argparse
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

    def run(self):
        try:
            admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
            topic = NewTopic(name="TweetStreamAnalyzer",
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic])
        except Exception as e:
            print("Create Kafka topic failed: " + str(e))

        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda x: dumps(x).encode('utf-8'))

        conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
        sc = SparkContext(conf=conf)

        # Creating a streaming context with batch interval of 10 sec
        ssc = StreamingContext(sc, 10)
        ssc.checkpoint("checkpoint")

        kafkaStream = KafkaUtils.createDirectStream(
            ssc, topics=["TweetStreamListener"], kafkaParams={"metadata.broker.list": 'localhost:9092'})
        tweets = kafkaStream.map(lambda x: x[1])

        # Each element of tweets will be the text of a tweet.
        # We keep track of a running total counts and print it at every time step.
        words = tweets.flatMap(lambda line: line.split(" "))
        positive = words.map(lambda word: ('Positive', 1) if word in self.posDict else ('Positive', 0))
        negative = words.map(lambda word: ('Negative', 1) if word in self.negDict else ('Negative', 0))
        allSentiments = positive.union(negative)
        sentimentCounts = allSentiments.reduceByKey(lambda x, y: x + y)

        def updateFunction(newValues, runningCount):
            if runningCount is None:
                runningCount = 0
            return sum(newValues, runningCount)

        runningSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)
        runningSentimentCounts.pprint(5)


        def handler(message):
            records = message.collect()
            for record in records:
                producer.send('TweetStreamAnalyzer', str(record))
                producer.flush()

        #kafkaStream.foreachRDD(handler)

        # # The counts variable hold the word counts for all time steps
        # counts = []
        # sentimentCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))



        # Start the computation
        duration = 100
        ssc.start()
        ssc.awaitTerminationOrTimeout(duration)
        ssc.stop(stopGraceFully=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="TweetStreamListener")

    args = parser.parse_args()

    def load_wordlist(filename):
        """
        This function returns a list or set of words from the given filename.
        """
        words = {}
        f = open(filename, 'r')
        text = f.read()
        text = text.split('\n')
        for line in text:
            words[line] = 1
        f.close()
        return words


    LOCAL_ROOT = os.path.abspath("data") + os.sep

    pwords = load_wordlist(LOCAL_ROOT + "positive.txt")
    nwords = load_wordlist(LOCAL_ROOT + "negative.txt")

    # LOCAL_ROOT = os.path.abspath("data") + os.sep
    # with open(LOCAL_ROOT + 'sentiment_pos_dict.pickle', 'rb') as handle:
    #     posDict = pickle.load(handle)
    #
    # with open(LOCAL_ROOT + 'sentiment_neg_dict.pickle', 'rb') as handle:
    #     negDict = pickle.load(handle)


    tweetsStreamAnalyzer = TweetStreamAnalyzer(upstreamTopic=args.topic, posDict=pwords, negDict=nwords)
    tweetsStreamAnalyzer.run()





if __name__ == "__main__":
    main()