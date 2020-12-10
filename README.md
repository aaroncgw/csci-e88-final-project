
1. Install required python packages:
pipenv install and pipenv shell

2. Start Zookeeper and Kafka:
docker-compose -f docker-compose.yml up -d

3. Run tweets listener.py 
python tweets_listener.py

4. Run tweets_analyzer with Spark:
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 tweets_analyzer.py

5. Run sentiments listener
python sentiments_listener.py
