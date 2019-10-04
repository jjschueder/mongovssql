# -*- coding: utf-8 -*-
#from __future__ import print_function
"""
Created on Wed Sep 25 13:58:25 2019

@author: jjschued
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 16:22:43 2019

@author: jjsch
"""




import json
import sys

import pandas as pd



woeidus = '23424977'



#Consumer API keys
consumerkey = ''
#(API key)
apisecretkey = ''
# (API secret key)
#Access token & access token secret
accesstoken = ''
# (Access token)
accesstokensecret = ''
# (Access token secret)







import tweepy
consumer_key = consumerkey
consumer_secret = apisecretkey
access_token = accesstoken
access_token_secret = accesstokensecret
#Now it’s time to create our API object.

# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object while passing in auth information
api2 = tweepy.API(auth)




listOfTweets = []
for tweet in tweepy.Cursor(api2.search, q='#metoo', rpp=100).items(100): 
       dict_ = { 'twitterid' : tweet.id,
                'Screen Name': tweet.user.screen_name,
                'User Name': tweet.user.name,
                'Tweet Created At': tweet.created_at,
                'Tweet Text': tweet.text,
                'User Location': tweet.user.location,
                'Tweet Coordinates': tweet.coordinates,
                'Retweet Count': tweet.retweet_count,
                'Retweeted': tweet.retweeted,
                'Phone Type': tweet.source,
                'Favorite Count': tweet.favorite_count,
                'Favorited': tweet.favorited,
                'Replied': tweet.in_reply_to_status_id_str
                }
       listOfTweets.append(dict_)
dfx = pd.DataFrame(listOfTweets)

#example of retrieving data from a mysql database
import mysql.connector
cnx = mysql.connector.connect(user='root', password='',
                              host='localhost',
                              database='twitter')
cursor = cnx.cursor()

add_tweet = ("INSERT INTO twitterdata "
            "(TWITTERKEY, favoritecount, favorited, phonetype, replied, retweetcount, retweeted, screenname, tweetcoordinate, tweetcreatedat, tweet, userlocation, username) "
            "VALUES (%(TWITTERKEY)s, %(favoritecount)s, %(favorited)s, %(phonetype)s, %(replied)s, %(retweetcount)s, %(retweeted)s, %(screenname)s, %(tweetcoordinate)s, %(tweetcreatedat)s, %(tweet)s, %(userlocation)s, %(username)s)")


add_tweet = ("INSERT INTO twitterdata "
            "(TWITTERKEY, favoritecount, favorited, phonetype, replied, retweetcount) "
            "VALUES (%(TWITTERKEY)s, %(favoritecount)s, %(favorited)s, %(phonetype)s, %(replied)s, %(retweetcount)s)")




for index, row in dfx.iterrows():
     print(row['twitterid'])
     data_twitter = {
          'TWITTERKEY' : row['twitterid'],  
          'favoritecount' : row['Favorite Count'],
          'favorited' : row['Favorited'],
          'phonetype' : row['Phone Type'],
          'replied' : row['Replied'],
          'retweetcount' : row['Retweet Count'],
          'retweeted' : row['Retweeted'], 
          'screenname' : row['Screen Name'], 
          'tweetcoordinate': row['Tweet Coordinates'], 
          'tweetcreatedat' : row['Tweet Created At'],
          'tweet' : row['Tweet Text'], 
          'userlocation' : row['User Location'], 
          'username' : row['User Name'],

     }
     cursor.execute(add_tweet, data_twitter)    
cnx.commit()

cursor.close()
cnx.close()
     

#example selecting from a mysql db
#example of retrieving data from a mysql database
#import mysql.connector
#cnx = mysql.connector.connect(user='root', password='Lejms!2007',
#                              host='localhost',
#                              database='sakila')
#cursor = cnx.cursor()
#results = ''
#query = ("SELECT * FROM sakila.film_actor;")
#cursor.execute(query)
#fieldnames = cursor.description
#res_list = [x[0] for x in fieldnames]
#msgs = []
#msg =[]
#for (actor_id, film_id, last_update) in cursor:
#    print("{}, {}, {:%d %b %Y}".format (
#    actor_id, film_id, last_update))
#    msg = [actor_id, film_id, last_update] 
#    msg = tuple(msg)                    
#    msgs.append(msg)
#dfmysql = pd.DataFrame(msgs)
#for col in dfmysql.columns:
#        dfmysql = dfmysql.rename(columns={col: res_list[col]})
#
#add_employee = ("INSERT INTO employees "
#               "(first_name, last_name, hire_date, gender, birth_date) "
#               "VALUES (%s, %s, %s, %s, %s)")
#
#for col in dfx:
#  print(dfmysql[col,"twitterid"])
#  data_twitter = {
#  'emp_no': emp_no,
#  'salary': 50000,
#  'from_date': tomorrow,
#  'to_date': date(9999, 1, 1),
#}
 

        
#example sql create for table that will be written to in mysql
#use twitter;
#
#CREATE TABLE `twitterdata` (
#  `TWITTERKEY` bigint NOT NULL,
#  `favoritecount` int DEFAULT NULL,
#  `favorited` varchar(45) DEFAULT NULL,
#  `phonetype` varchar(45) DEFAULT NULL,
#  `replied` varchar(100) DEFAULT NULL,
#  `retweetcount` int DEFAULT NULL,
#   `retweeted` varchar(45) DEFAULT NULL,
#   `screenname` varchar(45) DEFAULT NULL,
#   `tweetcoordinate` varchar(45) DEFAULT NULL,
#   `tweetcreatedat` datetime DEFAULT NULL,
#  `tweet` text,
#  `userlocation` varchar(100) DEFAULT NULL,
#  `username` varchar(100) DEFAULT NULL,
#  PRIMARY KEY (`TWITTERKEY`)
#) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci;
#     
##produces a list of trending topics for the United States
#trends = api2.trends_place(woeidus)
#trend2 = trends[0]['trends']
#dftrend = pd.DataFrame(trend2)
#
#
##produces the top 20 tweets on my homepage (based on the api keys) 
#public_tweets = api2.home_timeline()
#ptweets = public_tweets[0]
##ptweetsdf = pd.DataFrame(ptweets)
#
#for tweet in public_tweets:
#   # printing the text stored inside the tweet object
##   print (tweet.text)
#   print (tweet.user.screen_name)
   
#pubtweetsdf = pd.DataFrame(public_tweets)

##search for a topic and add to dataframe
#dfx = pd.DataFrame(columns=['text', 'source', 'url'])
#msgs = []
#msg =[]
#
#for tweet in tweepy.Cursor(api2.search, q='#metoo', rpp=100).items(100):
#    msg = [tweet.text, tweet.source, tweet.source_url] 
#    msg = tuple(msg)                    
#    msgs.append(msg)  
#dfx = pd.DataFrame(msgs)
