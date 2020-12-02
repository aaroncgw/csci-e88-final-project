import os
import pickle
from datetime import datetime

from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, FloatType
from pyspark.sql.functions import udf, from_json, col, window, unix_timestamp
from pyspark.ml.feature import StopWordsRemover, Tokenizer


class TweetStreamAnalyzer:
    def __init__(self):
        LOCAL_ROOT = os.path.abspath("data") + os.sep
        with open(LOCAL_ROOT + 'sentiment_pos_dict.pickle', 'rb') as handle:
            self.posDict = pickle.load(handle)

        with open(LOCAL_ROOT + 'sentiment_neg_dict.pickle', 'rb') as handle:
            self.negDict = pickle.load(handle)

    def calc_sentiment(self, words):
        positive = 0
        negative = 0
        for word in words:
            if word in self.posDict:
                positive = positive + 1
            elif word in self.negDict:
                negative = negative + 1
        if len(words) > 0 and (positive + negative) > 0:
            return (positive - negative) / (positive + negative) / len(words)
        else:
            return 0

    def run(self):
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

        date_process = udf(
            lambda x: datetime.strftime(
                datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'
            )
        )

        tweets_table = tweets_table.withColumn("created_at", date_process(tweets_table['created_at']))
        tweets_table = tweets_table.withColumn("created_at", unix_timestamp('created_at', "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))

        tokenizer = Tokenizer(inputCol="tweet", outputCol="words")
        tokenized = tokenizer.transform(tweets_table)
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        removed = remover.transform(tokenized)

        calculate_sentiment = udf(self.calc_sentiment, FloatType())
        cleaned_df = removed.withColumn("sentiment", calculate_sentiment("filtered"))

        df = cleaned_df \
            .groupBy(window(cleaned_df.created_at, "1 minutes")).avg("sentiment")

        df = df.withColumn("sentiment", col("avg(sentiment)"))

        checkpoint = os.path.abspath("tmp") + "/checkpoint"

        query = df \
            .selectExpr("CAST(window AS STRING) as key", "CAST(sentiment AS STRING) as value") \
            .writeStream \
            .trigger(processingTime = "1 minute") \
            .format("kafka") \
            .outputMode("complete") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "TweetStreamAnalyzer") \
            .option("checkpointLocation", checkpoint) \
            .start()

        query.awaitTermination()



if __name__ == "__main__":
    tweetStreamAnalyzer = TweetStreamAnalyzer()
    tweetStreamAnalyzer.run()