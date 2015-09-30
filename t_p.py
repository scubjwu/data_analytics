from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pymongo import MongoClient
import json
import time
import random
import os
import pymongo
from Queue import Queue
from threading import Thread
import csv
import tweepy
import logging

logger = logging.getLogger('test_log')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('test.log', mode='a')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s : %(message)s @ %(thread)d')
fh.setFormatter(formatter)
logger.addHandler(fh)

ckey = "donoBSHOjnHt4YPG3avSmUvro"
csecret = "RARzNyiPPkg8U6DBXofQM4sbrERiz7xPFii9bJXi7RVm79IFpO"
atoken = "2509388425-pZiEKSsbY4rIlCVr0Zl6N6LU5ICRdz283ACu9is"
asecret = "7EGL5FXVkwZICn8sIjxxkTBCOSs7oEvLJTAvpCdsTvDcZ"

client = MongoClient()
db = pymongo.database.Database(client, "test")
collection_tweet = pymongo.collection.Collection(db, "tweet_NYC")

collection_user = pymongo.collection.Collection(db, "user_NYC")
collection_user.create_index("id", unique=True)

uq = Queue(maxsize = 0)

my_twitter = []

class twitter_T:
	def __init__(self, name, ckey, csecret, atoken, asecret):
	    self.name = name
		
	    self.ckey = ckey
	    self.csecret = csecret
	    self.atoken = atoken
	    self.asecret = asecret
		
	    #init the api handler
	    auth = tweepy.OAuthHandler(self.ckey, self.csecret)
	    auth.set_access_token(self.atoken, self.asecret)
	    self.t_api = tweepy.API(auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

	def followers_lst(self, user_id):
	    lst = []
	    tmp = tweepy.Cursor(self.t_api.followers_ids, user_id).pages()
	    while True:
		try:
                    page = tmp.next()
                    lst.extend(page)
                        #the user not auth us to extract his relationship or he does not follow anyone....
		except tweepy.TweepError as e:
                    if 'Failed to send request' in str(e):
                        logger.error(e)
                        time.sleep(30)
                        continue
                    else:
                        logger.warn(e)

                        if e.response.status_code == 401:
                            logger.info('user %d not authorized', user_id)
                            return None
                        elif e.response.status_code == 404:
                            logger.info('user %d has no followers', user_id)
                            return []
                        else:
                            logger.error(e.reason)
                            time.sleep(30)
                            continue

		except StopIteration:
		    break
                
            return lst
	
	def friends_lst(self, user_id):
	    lst = []
	    tmp = tweepy.Cursor(self.t_api.friends_ids, user_id).pages()
	    while True:
		try:
		    page = tmp.next()
		    lst.extend(page)
		except tweepy.TweepError as e:
                    if 'Failed to send request' in str(e):
                        logger.error(e)
                        time.sleep(30)
                        continue
                    else:
                        logger.warn(e)

                        if e.response.status_code == 401:
                            logger.info('user %d not authorized', user_id)
                            return None
                        elif e.response.status_code == 404:
                            logger.info('user %d has no friends', user_id)
                            return []
                        else:
                            logger.error(e.reason)
                            time.sleep(30)
                            continue

		except StopIteration:
		    break
	    
            return lst

def twitter_init(lst):
    with open("auth_info.csv") as fp:
        line = csv.reader(fp, delimiter=',')
        for row in line:
            ckey = row[0]
            csecret = row[1]
            atoken = row[2]
            asecret = row[3]
            twitter = twitter_T('user_info', ckey, csecret, atoken, asecret)
            lst.append(twitter)

def retrive_user_friendship(Q, twitter_h):
    while True:
        if Q.empty():
            continue

        user_id = Q.get()
        lst_fo = twitter_h.followers_lst(user_id)
        lst_fr = twitter_h.friends_lst(user_id)

        new_info = {}
        new_info['friends_id'] = lst_fr
        new_info['followers_id'] = lst_fo

        try:
            collection_user.update_one({"id": user_id}, {'$set' : new_info})
        except pymongo.errors as e:
            print 'user db update error'
            logger.error(e)

        logger.debug("user %d has beed added", user_id)
        Q.task_done()


class listener(StreamListener):
	def on_data(self, data):
                tweet = json.loads(data)
                coor = tweet["place"]["bounding_box"]["coordinates"][0]
                loc_point = {"loc_point" : {"type" : "Point", "coordinates" : [(coor[0][0] + coor[2][0])/2, (coor[0][1] + coor[2][1])/2]}}
                tweet.update(loc_point)

                try:
                    collection_tweet.insert_one(tweet)
                except pymongo.errors as e:
                    print 'tweet db insert error'
                    logger.error(e)

                user_info = tweet["user"]
                flag = 0
                try:
                    collection_user.insert_one(user_info)
                except pymongo.errors.DuplicateKeyError:
                    flag = 1

                if (flag == 0):
                    #put the new user infor into queue to retrive user friendship list
                    logger.debug("process user %d infor", user_info["id"])
                    uq.put(user_info["id"])
                else:
                    logger.debug("user %d infor already in db", user_info["id"])


	def on_error(self, status):
		print "error ", status

def main():
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    twitterStream = Stream(auth, listener())

    twitter_init(my_twitter)
    thread_num = len(my_twitter)

    for i in range(thread_num):
        worker = Thread(target=retrive_user_friendship, args=(uq, my_twitter[i],))
        worker.setDaemon(True)
        worker.start()

    while True:
        try:
            print "start..."
            #NYC
            twitterStream.filter(locations=[-74,40,-73,41])
        except KeyboardInterrupt:
            print "stop..."
            uq.join()
            exit(0)
        except Exception, e:
            print "Hang..."
            logger.error(e)
            time.sleep(random.randint(10,30));

if __name__ == '__main__':
    main()

