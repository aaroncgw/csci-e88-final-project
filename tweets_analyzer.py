import os
import re
import pickle
from datetime import datetime

from pyspark.sql.types import StringType, StructType, StructField, TimestampType, DoubleType, ArrayType, ByteType, IntegerType
from pyspark.sql.functions import udf, from_json, Column, window, unix_timestamp, col, slice


from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression


class TweetStreamAnalyzer:
    def __init__(self):
        self.train_model()

    def train_model(self):
        sc = SparkContext(appName="PySparkShell")
        spark = SparkSession(sc)

        train_data_schema = tp.StructType([
            tp.StructField(name='id', dataType=tp.IntegerType(), nullable=True),
            tp.StructField(name='label', dataType=tp.IntegerType(), nullable=True),
            tp.StructField(name='tweet', dataType=tp.StringType(), nullable=True)
        ])

        LOCAL_ROOT = os.path.abspath("data") + os.sep

        train_data = spark.read.csv(LOCAL_ROOT + 'twitter_sentiments_train.csv',
                                 schema=train_data_schema, header=True)

        stage_1 = RegexTokenizer(inputCol='tweet', outputCol='tokens', pattern='\\W')
        stage_2 = StopWordsRemover(inputCol='tokens', outputCol='filtered_words')
        stage_3 = Word2Vec(inputCol='filtered_words', outputCol='vector', vectorSize=100)
        model = LogisticRegression(featuresCol='vector', labelCol='label')
        pipeline = Pipeline(stages=[stage_1, stage_2, stage_3, model])
        self.pipelineFit = pipeline.fit(train_data)

        print("Done!")

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

        # map message to two columns created_at and tweet which are the values coming from tweet_listener through Kafka
        tweets_table = kafka_df.withColumn("message", from_json(col("message"), schema)).select("timestamp", "message.*")

        date_process = udf(
            lambda x: datetime.strftime(datetime.strptime(x, '%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S')
        )
        tweets_table = tweets_table.withColumn("created_at", date_process(tweets_table['created_at']))
        tweets_table = tweets_table.withColumn("created_at", unix_timestamp('created_at', "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))

        # remove weblink and special characters
        pre_process = udf(
            lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', x.lower().strip()), StringType()
        )
        tweets_table = tweets_table.withColumn("cleaned_data", pre_process(tweets_table.tweet)).dropna()
        prediction = self.pipelineFit.transform(tweets_table).select("created_at", "probability")

        # extract the 1(positive) sentiment probability
        def extract_prob(v):
            try:
                return float(v[1])  # VectorUDT is of length 2
            except ValueError:
                return None

        extract_prob_udf = udf(extract_prob, DoubleType())
        prediction = prediction.withColumn("probability", extract_prob_udf(col("probability")))

        # window operation: calculate the average probability in 1 minute window
        df = prediction \
            .groupBy(window(prediction.created_at, "1 minutes")).avg("probability")
        df = df.withColumn("sentiment", col("avg(probability)"))

        checkpoint = os.getcwd() + "/checkpoint"
        query = df \
            .selectExpr("CAST(window AS STRING) as key", "CAST(sentiment AS STRING) as value") \
            .writeStream \
            .trigger(processingTime = "1 minute") \
            .format("kafka") \
            .outputMode("complete") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "TweetStreamSentiments") \
            .option("checkpointLocation", checkpoint) \
            .start()

        query.awaitTermination()



if __name__ == "__main__":
    tweetStreamAnalyzer = TweetStreamAnalyzer()
    tweetStreamAnalyzer.run()