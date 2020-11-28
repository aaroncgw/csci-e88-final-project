import datetime
import tweepy


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        now = datetime.datetime.now()
        print(now.strftime("%Y-%m-%d %H:%M:%S"))
        print(status.text)


