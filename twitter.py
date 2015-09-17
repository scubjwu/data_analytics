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
                            print "Error in followers_lst: ", e.reason
                            return [] 
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
                            print "Error in friends_lst: ", e.reason
                            return []
			except StopIteration:
			    break
		return lst

#=================================global infor=======================================#
my_twitter = []

ckey0 = "OVJ62utfPINH1tjJlX3gACqAo"
csecret0 = "W0zw7Y4zWATZwRiBKeVLsHNTHwrbui1IekH9f5vqInRXEHyutM"
atoken0 = "3688208117-QDc0CWpLe24EETnKDVZ28G6pCv7Md53A50Bb5Uk"
asecret0 = "3MG2tAHiraigWwX5U3nq0ECDqDFMjuRrEm66DpD7rLwyj"
twitter0 = twitter_T('t_user0', ckey0, csecret0, atoken0, asecret0)
my_twitter.append(twitter0)

ckey1 = "donoBSHOjnHt4YPG3avSmUvro"
csecret1 = "RARzNyiPPkg8U6DBXofQM4sbrERiz7xPFii9bJXi7RVm79IFpO"
atoken1 = "2509388425-pZiEKSsbY4rIlCVr0Zl6N6LU5ICRdz283ACu9is"
asecret1 = "7EGL5FXVkwZICn8sIjxxkTBCOSs7oEvLJTAvpCdsTvDcZ"
twitter1 = twitter_T('t_user1', ckey1, csecret1, atoken1, asecret1)
my_twitter.append(twitter1)

ckey2 = "7nWywVmFealeMvswit3q8gmK0"
csecret2 = "dGCzEqccsMadjlJaF1wdLXYVarGfUmYOGdsAlT4d2Fng3uRXAh"
atoken2 = "3688208117-wgKdmVnCFuMkIJT5U8NzUKKuBhpX6G5xy45zFBW"
asecret2 = "Twytg9TkwByuXU2YllAwFWcZKRO7xbAMKY8MpgSa6bUKu"
twitter2 = twitter_T('t_user2', ckey2, csecret2, atoken2, asecret2)
my_twitter.append(twitter2)

ckey3 = "veCUf9FagS16QmG0PWkXRVMLa"
csecret3 = "iX6GSn8NG6GeBGDT7OUKYmOmlnCGUlj8cOQn91ebHjXEwtAybY"
atoken3 = "2509388425-H2EXibllWS0jfp3N0ShOiPJxnGSAuet5EOM7b25"
asecret3 = "DnNO0S1DHYsrBofYZGdJlbWpxctTfNS2mjtynyxpfXyQp"
twitter3 = twitter_T('t_user3', ckey3, csecret3, atoken3, asecret3)
my_twitter.append(twitter3)

ckey4 = "S5Rga6CAX6KyiHki7UsxCqzxj"
csecret4 = "j7NBQHHSDGUgLif795UiBq6cpCFiOVjeDbn4D6EArtolKXgpod"
atoken4 = "2509388425-ijsseTtnXXnt6SqGy0PhVTGOZgjFkA9GCFX4dY6"
asecret4 = "qXGpDyGYl4W5VNLfjKvrV47vfAHx41p0Xk143ir6EAt1j"
twitter4 = twitter_T('t_user4', ckey4, csecret4, atoken4, asecret4)
my_twitter.append(twitter4)

ckey5 = "MkzQu5X3j1WSLGnggcmq7DYzi"
csecret5 = "5zzM5rISHOO8H6q1HxqrcOzpXjHoap0ax2mgQk6JcNZtMvDpDU"
atoken5 = "3688208117-2zXF1SuaXAh7uhoDdPcQGCC73EE2woEjhLxFp76"
asecret5 = "9oMNO5ySUC5IXw1wKqK4RvmUrtJ48KdbYoni9I3dKFqx6"
twitter5 = twitter_T('t_user5', ckey5, csecret5, atoken5, asecret5)
my_twitter.append(twitter5)

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
            print "insert a new doc..."
            collection_user.insert_one(new_doc)

def process_cursor(cursor, twitter_h):
    for document in cursor:
        process_document(document, twitter_h)

def main():
    tweet_client = MongoClient()
    tweet_db = pymongo.database.Database(tweet_client, "test")
    tweet_collection = pymongo.collection.Collection(tweet_db, "tweet_NYC")

    cursors = tweet_collection.parallel_scan(6)
    threads = []
    for i in range(6):
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
