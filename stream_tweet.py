# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 19:17:14 2020

@author: vishwateja.r
"""


from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import boto3
from datetime import date
import uuid


#consumer key, consumer secret, access token, access secret.

ckey= '********************************************'
csecret= ''********************************************''
atoken= ''********************************************''
asecret= ''********************************************''



s3 = boto3.client('s3')


class listener(StreamListener):

    def on_data(self, data):
        all_data = json.loads(data)

        tweet = all_data["text"]
        #print(all_data)
        username = all_data["user"]["screen_name"]
        print(username)
        
        values = {"uname":username,"id":all_data['id'],"data":tweet} 
        #values=json.dumps(values)
        today = date.today()
        today = today.strftime("%Y/%m/%d")
        u_id=uuid.uuid1()
        key_name= today+'/'+ str(u_id)+'_'+username+'.json'
        s3.put_object(Body=str(values), Bucket='twitter-s3', Key=key_name)


        return True

    def on_error(self, status):
        print(status)

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
tags = ['#emo']
twitterStream.filter(track=tags)

