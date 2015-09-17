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

class twitter_T:
	ckey = ""
	csecret = ""
	atoken = ""
	asecret = ""
	t_api = None

	def __init__(self, name, ckey, csecret, atoken, asecret):
		self.name = name
		
		if twitter_T.t_api is not None:
			print "already init..."
			return

		twitter_T.ckey = ckey
		twitter_T.csecret = csecret
		twitter_T.atoken = atoken
		twitter_T.asecret = asecret
		
		#init the api handler
		auth = tweepy.OAuthHandler(self.ckey, self.csecret)
		auth.set_access_token(self.atoken, self.asecret)
		twitter_T.t_api = tweepy.API(auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

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
			except tweepy.TweepError:
                            print "!ERROR:when extract friends list: ", tweepy.TweepError
			    time.sleep(60*15)
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
			except tweepy.TweepError:
                            print "!ERROR:when extract friends list: ", tweepy.TweepError
			    time.sleep(60*15)
			    continue
			except StopIteration:
			    break
		return lst

#=================================global infor=======================================#
my_ckey = "veCUf9FagS16QmG0PWkXRVMLa"
my_csecret = "iX6GSn8NG6GeBGDT7OUKYmOmlnCGUlj8cOQn91ebHjXEwtAybY"
my_atoken = "2509388425-H2EXibllWS0jfp3N0ShOiPJxnGSAuet5EOM7b25"
my_asecret = "DnNO0S1DHYsrBofYZGdJlbWpxctTfNS2mjtynyxpfXyQp"
my_twitter = twitter_T('t_user', my_ckey, my_csecret, my_atoken, my_asecret)

client_user = MongoClient()
db_user = pymongo.database.Database(client_user, "test")
collection_user = pymongo.collection.Collection(db_user, "user_NYC")
collection_user.create_index("user_id")

db_lock = threading.Lock()
#======================================================================================#
def process_document(document):
    user_info = document['user']

    user_id = json.loads(user_info['id_str'])
    res = None

    with db_lock:
        res = collection_user.find_one({'user_id':user_id})

    if res is not None:
        return

    lst_fo = my_twitter.followers_lst(user_id)
    lst_fr = my_twitter.friends_lst(user_id)

    new_doc = {}
    new_doc['user_id'] = user_id
    new_doc['friends_id'] = lst_fr
    new_doc['followers_id'] = lst_fo

    with db_lock:
        res = collection_user.find_one({'user_id':user_id})
        if res is None:
            print "insert a new doc..."
            collection_user.insert_one(new_doc)

def process_cursor(cursor):
    for document in cursor:
        process_document(document)

def main():
    my_twitter.twitter_DB('test', 'tweet_NYC')
    cursors = my_twitter.collection.parallel_scan(4)
    threads = [
            threading.Thread(target=process_cursor, args=(cursor,))
            for cursor in cursors]

    print "Start..."

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print "Done!"

if __name__ == '__main__':
    main()

"""
my_twitter.twitter_DB('test', 'tweet_NYC')
res = my_twitter.collection.find_one({"id_str":"636433425336037376"})
#extract and construct the followers and friends infor of this user
user_info = res['user']
user_id = json.loads(user_info['id_str'])
lst_fo = my_twitter.followers_lst(user_id)
lst_fr = my_twitter.friends_lst(user_id)

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
