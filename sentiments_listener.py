import os
import pykafka
from pykafka.common import OffsetType
from elasticsearch import Elasticsearch
import json
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.types as tp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row, Column

def main():
    es = Elasticsearch('https://db2cb7cbe8834bb1a48f960a437f461d.us-east-1.aws.found.io:9243',
                    http_auth=(os.environ["ELASTIC_USERNAME"], os.environ["ELASTIC_PASSWORD"]))

    client = pykafka.KafkaClient("localhost:9092")
    topic = client.topics[bytes("TweetStreamAnalyzer", 'ascii')]
    consumer = topic.get_simple_consumer(consumer_group="mygroup",
                                        auto_offset_reset=OffsetType.EARLIEST,
                                        reset_offset_on_start=False)

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
    # sc = SparkContext(appName="PySparkShell")
    # spark = SparkSession(sc)
    #
    # # define the schema
    # my_schema = tp.StructType([
    #     tp.StructField(name='id', dataType=tp.IntegerType(), nullable=True),
    #     tp.StructField(name='label', dataType=tp.IntegerType(), nullable=True),
    #     tp.StructField(name='tweet', dataType=tp.StringType(), nullable=True)
    # ])
    # # reading the data set
    # print('\n\nReading the dataset...........................\n')
    # my_data = spark.read.csv('./data/twitter_sentiments.csv',
    #                          schema=my_schema, header=True)
    # my_data.show(2)
    #
    # my_data.printSchema()
    # print('\n\nDefining the pipeline stages.................\n')
    # stage_1 = RegexTokenizer(inputCol='tweet', outputCol='tokens', pattern='\\W')
    #
    # stage_2 = StopWordsRemover(inputCol='tokens', outputCol='filtered_words')
    #
    # stage_3 = Word2Vec(inputCol='filtered_words', outputCol='vector', vectorSize=100)
    #
    # model = LogisticRegression(featuresCol='vector', labelCol='label')
    #
    # print('\n\nStages Defined................................\n')
    # pipeline = Pipeline(stages=[stage_1, stage_2, stage_3, model])
    #
    # print('\n\nFit the pipeline with the training data.......\n')
    # pipelineFit = pipeline.fit(my_data)
    #
    # print("Done!")

