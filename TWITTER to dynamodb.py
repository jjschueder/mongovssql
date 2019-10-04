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
     
import boto3

token = get_aws_credentials()  # you have to get this part its not in the code here!!

AccessKeyId = token['Credentials']['AccessKeyId']
SecretAccessKey = token['Credentials']['SecretAccessKey']
SessionToken =token['Credentials']['SessionToken']


dynamodb = boto3.resource('dynamodb',
                          aws_session_token=SessionToken,
                          aws_access_key_id=AccessKeyId,
                          aws_secret_access_key=SecretAccessKey,
                          region_name="us-east-1"
)


dynamodbclient = boto3.client('dynamodb',
                          aws_session_token=SessionToken,
                          aws_access_key_id=AccessKeyId,
                          aws_secret_access_key=SecretAccessKey,
                          region_name="us-east-1"
)

table = dynamodb.Table('twitterscrape')

for index, row in dfx.iterrows():
   
         if row['User Location'] == '':
             row['User Location'] = 'None'
         response = table.put_item(
                 Item={                         
                      'twitterid' : row['twitterid'],  
                      'favoritecount' : row['Favorite Count'],
                      'favorited' : row['Favorited'],
                      'phonetype' : row['Phone Type'],
                      'replied' : row['Replied'],
                      'retweetcount' : row['Retweet Count'],
                      'retweeted' : row['Retweeted'], 
                      'screenname' : row['Screen Name'], 
                      'tweetcoordinate': row['Tweet Coordinates'], 
                      'tweetcreatedat' : str(row['Tweet Created At']),
                      'tweet' : row['Tweet Text'], 
                      'userlocation' : row['User Location'], 
                      'username' : row['User Name']
    }
)
         
print("Total Duration was: ", datetime.now() - starttime) 



