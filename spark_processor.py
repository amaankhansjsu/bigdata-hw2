from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RedditSentimentAnalysis") \
    .getOrCreate()

# Create Kafka Stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "reddit-stream") \
    .load()

# Deserialize JSON
df = df.selectExpr("CAST(value AS STRING)")

# Sentiment Analysis Function
analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    score = analyzer.polarity_scores(text)['compound']
    return "positive" if score > 0.05 else "negative" if score < -0.05 else "neutral"

sentiment_udf = udf(get_sentiment, StringType())

# Apply Sentiment Analysis
df = df.withColumn("sentiment", sentiment_udf(col("value")))

# Write to Console (For Debugging)
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Write to Parquet
df.write \
    .format("parquet") \
    .mode("append") \
    .saveAsTable("reddit_sentiment")

query.awaitTermination()
