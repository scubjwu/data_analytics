import time
import random
import os
import tweepy

ckey = "donoBSHOjnHt4YPG3avSmUvro"
csecret = "RARzNyiPPkg8U6DBXofQM4sbrERiz7xPFii9bJXi7RVm79IFpO"
atoken = "2509388425-pZiEKSsbY4rIlCVr0Zl6N6LU5ICRdz283ACu9is"
asecret = "7EGL5FXVkwZICn8sIjxxkTBCOSs7oEvLJTAvpCdsTvDcZ"

"""
if os.path.exists("./tweet.csv"):
    os.remove("./tweet.csv")
db = file("./tweet.csv", "a")
"""

#TODO: work with DB!

def main():
    auth = tweepy.OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    api = tweepy.API(auth_handler=auth)

    u = api.trends_available()
    print u

#best way to retrive the tweet infor
    f_list = []
    p = tweepy.Cursor(api.followers_ids, user_id="2355933535").pages()
    while True:
        try:
            d = p.next()
            f_list.extend(d)
        except tweepy.TweepError:
            print "Eror..."
            time.sleep(10)
            continue
        except StopIteration:
            break

    print len(f_list)

"""
    c = tweepy.Cursor(api.followers, user_id="2355933535").items()
    while True:
        try:
            d = c.next()
            print d
        except tweepy.TweepError:
            #will get the rate limit error because at most 30 pages per 15 min....
            print "Error..."
            time.sleep(60)
            continue
        except StopIteration:
            break
"""

"""
    f_list = []
    for page in tweepy.Cursor(api.followers, user_id="2355933535").items():
        #f_list.extend(page)
        print page
        time.sleep(10)

    print len(f_list)
"""
"""
#this is a safe way to get all followers?
    ids1 = []
    for page in tweepy.Cursor(api.followers_ids, user_id="2355933535").pages():
        ids1.extend(page)
        time.sleep(10)

    ids2 = []
    ids2 = api.followers_ids(user_id="2355933535")

    print ids1
    print ids2
"""

if __name__ == '__main__':
    main()

