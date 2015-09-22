import time
import random
import os
import tweepy
import pymongo
import json
from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps
import bson
import collections
from bson.codec_options import CodecOptions
import threading
import csv
import logging

logger = logging.getLogger('test_log')
logger.setLevel(logging.INFO)

fh = logging.FileHandler('test.log', mode='a')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s : %(message)s')
fh.setFormatter(formatter)

logger.addHandler(fh)

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

	#def __del__(self):
	#	twitter_T.twitter_cnt -= 1

	def twitter_DB(self, db, collection):
	    client = MongoClient()
	    self.db = pymongo.database.Database(client, db)
	    self.collection = pymongo.collection.Collection(self.db, collection)
	
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
                        time.sleep(60)
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
                            time.sleep(60)
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
                        time.sleep(60)
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
                            time.sleep(60)
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

#=================================global infor=======================================#
my_twitter = []

client_user = MongoClient()
db_user = pymongo.database.Database(client_user, "test")
collection_user = pymongo.collection.Collection(db_user, "user_NYC")
collection_user.create_index("user_id")

db_lock = threading.Lock()
#======================================================================================#
def process_document(document, twitter_h):
    user_info = document['user']

    user_id = json.loads(user_info['id_str'])
    res = None

    with db_lock:
        res = collection_user.find_one({'user_id':user_id})

    if res is not None:
        logger.debug('find a duplicated docmentation')
        return

    lst_fo = twitter_h.followers_lst(user_id)
    lst_fr = twitter_h.friends_lst(user_id)

    new_doc = {}
    new_doc['user_id'] = user_id
    new_doc['friends_id'] = lst_fr
    new_doc['followers_id'] = lst_fo

    with db_lock:
        res = collection_user.find_one({'user_id':user_id})
        if res is None:
            logger.debug('insert a new docmentation')
            collection_user.insert_one(new_doc)

def process_cursor(cursor, twitter_h):
    for document in cursor:
        process_document(document, twitter_h)

def main():
    tweet_client = MongoClient()
    tweet_db = pymongo.database.Database(tweet_client, "test")
    tweet_collection = pymongo.collection.Collection(tweet_db, "tweet_NYC")

    twitter_init(my_twitter)
    parallel_num = len(my_twitter)
    cursors = tweet_collection.parallel_scan(parallel_num)
    threads = []

    for i in range(parallel_num):
        threads.append(threading.Thread(target=process_cursor, args=(cursors[i], my_twitter[i],)))

    print "Start..."
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
    print "Done!"

if __name__ == '__main__':
    main()

"""
my_twitter[3].twitter_DB('test', 'tweet_NYC')
res = my_twitter[3].collection.find_one({"id_str":"636433425336037376"})
#extract and construct the followers and friends infor of this user
user_info = res['user']
user_id = json.loads(user_info['id_str'])
lst_fo = my_twitter[3].followers_lst(user_id)
lst_fr = my_twitter[3].friends_lst(user_id)

user_test = {}
user_test['user_id'] = user_id
user_test['friends_id'] = lst_fr
user_test['followers_id'] = lst_fo
print user_test
#insert this user info if it is new
#res = collection_user.find_one({'user_id':user_id})
#if res is None:
#    collection_user.insert_one(user_test)
#    print "insert a new element"
#else:
#    print res
def process_cursor(cursor):
	for document in cursor:

cursors = test.collection.parallel_scan(4)
threads = [
	threading.Thread(target=process_cursor, args=(cursor,))
	for cursor in cursors]

for thread in threads:
	thread.start()

for thread in threads:
	thread.join()
"""
