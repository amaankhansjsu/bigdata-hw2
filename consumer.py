import praw
import json
from kafka import KafkaProducer

# Reddit API credentials
reddit = praw.Reddit(
    client_id="_wCs4ZJYjK22TC5febfqvg",
    client_secret="JQHMvr1gLchBXVf-y5hXv0QWOF680A",
    user_agent="Data_analysis"
)

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to fetch Reddit posts
def stream_reddit():
    subreddit = reddit.subreddit("worldnews")  
    for post in subreddit.stream.submissions():
        data = {
            "title": post.title,
            "selftext": post.selftext,
            "created_utc": post.created_utc
        }
        producer.send("reddit-stream", value=data)
        print(f"Sent: {data}")

# Run the stream
if __name__ == "__main__":
    stream_reddit()
