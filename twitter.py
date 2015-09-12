import time
import random
import os
import tweepy
import pymongo
import json
from pymongo import MongoClient
from bson import Binary, Code
from bson.json_util import dumps

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
		twitter_T.t_api = tweepy.API(auth_handler=auth)

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
				time.sleep(10)
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
				time.sleep(10)
				continue
			except StopIteration:
				break
		return lst

#testing......
my_ckey = "donoBSHOjnHt4YPG3avSmUvro"
my_csecret = "RARzNyiPPkg8U6DBXofQM4sbrERiz7xPFii9bJXi7RVm79IFpO"
my_atoken = "2509388425-pZiEKSsbY4rIlCVr0Zl6N6LU5ICRdz283ACu9is"
my_asecret = "7EGL5FXVkwZICn8sIjxxkTBCOSs7oEvLJTAvpCdsTvDcZ"

test = twitter_T('test', my_ckey, my_csecret, my_atoken, my_asecret)

test.twitter_DB('test', 't_NYC')
res = test.collection.find_one({"id_str":"636433425336037376"})
res_json = dumps(res)

user_info = res['user']
#print(res)

#test.collection.create_index("id_str")

lst_fr = test.followers_lst(user_info['id_str'])
#print "friends lst: ",  lst_fr

user_test = {}
user_test['user_id'] = user_info['id_str']
user_test['friends_id'] = lst_fr
print user_test

#res1 = test.collection.find_one({"id_str":"0"})
#print res1
#lst_fo = test.friends_lst("2355933535")

#print "follower lst: ", lst_fo

"""
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
