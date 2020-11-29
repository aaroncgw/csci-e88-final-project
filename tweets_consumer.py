import os
import argparse

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import matplotlib.pyplot as plt


# def make_plot(counts):
#     """
#     This function plots the counts of positive and negative words for each timestep.
#     """
#     positiveCounts = []
#     negativeCounts = []
#     time = []
#
#     for val in counts:
#         positiveTuple = val[0]
#         positiveCounts.append(positiveTuple[1])
#         negativeTuple = val[1]
#         negativeCounts.append(negativeTuple[1])
#
#     for i in range(len(counts)):
#         time.append(i)
#
#     posLine = plt.plot(time, positiveCounts, 'bo-', label='Positive')
#     negLine = plt.plot(time, negativeCounts, 'go-', label='Negative')
#     plt.axis([0, len(counts), 0, max(max(positiveCounts), max(negativeCounts)) + 50])
#     plt.xlabel('Time step')
#     plt.ylabel('Word count')
#     plt.legend(loc='upper left')
#     plt.show()


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


def wordSentiment(word, pwords, nwords):
    if word in pwords:
        return ('positive', 1)
    elif word in nwords:
        return ('negative', 1)


def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)


# def sendRecord(record):
#     connection = createNewConnection()
#     connection.send(record)
#     connection.close()


def stream(ssc, pwords, nwords, topic, duration=100):
    kafkaStream = KafkaUtils.createDirectStream(
        ssc, topics=[topic], kafkaParams={"metadata.broker.list": 'localhost:9092'})
    tweets = kafkaStream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # We keep track of a running total counts and print it at every time step.
    words = tweets.flatMap(lambda line: line.split(" "))
    positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))
    negative = words.map(lambda word: ('Negative', 1) if word in nwords else ('Negative', 0))
    allSentiments = positive.union(negative)
    sentimentCounts = allSentiments.reduceByKey(lambda x, y: x + y)
    runningSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)

    runningSentimentCounts.pprint(5)

    # The counts variable hold the word counts for all time steps
    counts = []
    sentimentCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

    # Start the computation
    ssc.start()
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="tweepy-stream")

    args = parser.parse_args()

    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)

    # Creating a streaming context with batch interval of 10 sec
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    LOCAL_ROOT = os.path.abspath("data") + os.sep

    pwords = load_wordlist(LOCAL_ROOT + "positive.txt")
    nwords = load_wordlist(LOCAL_ROOT + "negative.txt")
    counts = stream(ssc, pwords, nwords, args.topic)

    print("Guanwen")
    print(counts)
    #make_plot(counts)


if __name__ == "__main__":
    main()