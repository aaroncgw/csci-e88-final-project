import os
import argparse
import pickle
from json import dumps
from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, FloatType
from pyspark.sql.functions import udf, from_json, col, window, to_timestamp, unix_timestamp
from pyspark.sql import Row, Column

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Tokenizer, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression


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
        # words = tweets.flatMap(lambda line: line.split(" "))
        # positive = words.map(lambda word: ('Positive', 1) if word in self.posDict else ('Positive', 0))
        # negative = words.map(lambda word: ('Negative', 1) if word in self.negDict else ('Negative', 0))
        # allSentiments = positive.union(negative)
        # sentimentCounts = allSentiments.reduceByKey(lambda x, y: x + y)

        def handler(message):
            tweets = message.collect()
            for tweet in tweets:
                producer.send(self.kafka_topic, str(tweet))
                producer.flush()

        tweets.foreachRDD(handler)

        ssc.start()
        ssc.awaitTermination()
        ssc.stop(stopGraceFully=True)





def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream_topic", default="TweetStreamListener")
    #
    args = parser.parse_args()
    #
    LOCAL_ROOT = os.path.abspath("data") + os.sep
    with open(LOCAL_ROOT + 'sentiment_pos_dict.pickle', 'rb') as handle:
        posDict = pickle.load(handle)

    with open(LOCAL_ROOT + 'sentiment_neg_dict.pickle', 'rb') as handle:
        negDict = pickle.load(handle)
    #
    # tweetsStreamAnalyzer = TweetStreamAnalyzer(upstreamTopic=args.topic, posDict=posDict, negDict=negDict)
    # tweetsStreamAnalyzer.run()

    def fun(words):
        positive = 0
        negative = 0
        for word in words:
            if word in posDict:
                positive = positive + 1
            elif word in negDict:
                negative = negative + 1
        if len(words) > 0 and (positive + negative) > 0:
            return (positive - negative) / (positive + negative) / len(words)
        else:
            return 0

    spark = SparkSession.builder.appName("TweetStreamAnalyzer").getOrCreate()

    schema = StructType(
        [StructField("created_at", StringType()),
         StructField("tweet", StringType())]
    )

    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "TweetStreamListener") \
        .option("startingOffsets", "latest") \
        .option("header", "true") \
        .load() \
        .selectExpr("CAST(timestamp AS TIMESTAMP) as timestamp", "CAST(value AS STRING) as message")

    tweets_table = kafka_df.withColumn("message", from_json(col("message"), schema)).select("timestamp", "message.*")
    #
    date_process = udf(
        lambda x: datetime.strftime(
            datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'
        )
    )

    # # Changing datetime format
    tweets_table = tweets_table.withColumn("created_at", date_process(tweets_table['created_at']))
    tweets_table = tweets_table.withColumn("created_at", unix_timestamp('created_at', "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))

    tokenizer = Tokenizer(inputCol="tweet", outputCol="words")
    tokenized = tokenizer.transform(tweets_table)
    #
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    removed = remover.transform(tokenized)

    calculate_sentiment = udf(fun, FloatType())
    new_df = removed.withColumn("sentiment", calculate_sentiment("filtered"))

    df = new_df \
        .withWatermark("created_at", "1 seconds") \
        .groupBy(window(new_df.created_at, "1 minutes"), "created_at").count()

    #df = df.withColumn("sentiment", col("avg(sentiment)"))

    checkpoint = os.path.abspath("tmp") + "/delta/events"
    delta_output_path = os.path.abspath("tmp") + "/delta/events/_checkpoints/twitter_predictions"

    query = df \
        .selectExpr("CAST(window AS STRING) as key", "CAST(count AS STRING) as value") \
        .writeStream \
        .trigger(processingTime = "1 minute") \
        .format("kafka") \
        .outputMode("update") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "TweetStreamAnalyzer") \
        .option("checkpointLocation", checkpoint) \
        .start(delta_output_path)

    query.awaitTermination()



if __name__ == "__main__":
    main()