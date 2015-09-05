from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pymongo import MongoClient
import json
import time
import random
import os

ckey = "donoBSHOjnHt4YPG3avSmUvro"
csecret = "RARzNyiPPkg8U6DBXofQM4sbrERiz7xPFii9bJXi7RVm79IFpO"
atoken = "2509388425-pZiEKSsbY4rIlCVr0Zl6N6LU5ICRdz283ACu9is"
asecret = "7EGL5FXVkwZICn8sIjxxkTBCOSs7oEvLJTAvpCdsTvDcZ"

coords = dict()
xy = []

if os.path.exists("./tweet.json"):
    os.remove("./tweet.json")
db = file("./tweet.json", "a")

"""
conn = MongoClient('localhost', 27017)
db = conn['twitter_NYC_db']
collection = db['twitter_NYC_collection']
"""

class listener(StreamListener):
        def on_data(self, data):
            #tweet = json.load(data)
            #collection.insert(tweet)
            db.write(data)
            db.write('\n')

	def on_error(self, status):
		print "error ", status

def main():
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    twitterStream = Stream(auth, listener())
    while True:
        try:
            print "start..."
            #NYC
            twitterStream.filter(locations=[-74,40,-73,41])
        except KeyboardInterrupt:
            db.close()
            print "stop..."
            exit(0)
        except Exception, e:
            print "Hang..."
            time.sleep(random.randint(5,10));

if __name__ == '__main__':
    main()

