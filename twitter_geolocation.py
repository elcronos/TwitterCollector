import sys
import tweepy
import json
import time
import re
import hashlib

from time import sleep
from collections import OrderedDict
#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API
access_token = "MY_TOKEN"
access_token_secret = "TOKEN_SECRET"
consumer_key = "KEY"
consumer_secret = "SECRET"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

locs = [112.9,-45.3,154.7,-11.2] #Australia

class CustomStreamListener(tweepy.StreamListener):
    def on_status(self, tweet):
        try:
            # Anonimising data
            #Remove twitter urls
            text = re.sub('https:\/\/t.co\/[a-zA-Z0-9\-\.]+', '', tweet.text)
            #Remove usernames from text
            text= re.sub('@([A-Za-z0-9_]+)','',text)
            #Applying Hash functions
            id= hashlib.sha224(tweet.id_str).hexdigest()
            idUser= hashlib.sha224(tweet.author._json['id_str']).hexdigest()
            username= hashlib.sha224(tweet.author._json['screen_name']).hexdigest()
            coordinates = tweet._json['coordinates']

            # Coordinates are returned only if geo enable is True
            if(coordinates is None):
                lat = 'None'
                lon = 'None'
            else:
                lat = coordinates[u'coordinates'][1]
                lon = coordinates[u'coordinates'][0]

            data = OrderedDict([
                 ('id_tweet', id),
                 ('source',tweet.source),
                 ('created_at',str(tweet.created_at)),
                 ('location', tweet.author._json['location']),
                 ('lat', lat),
                 ('lon', lon),
                 ('time_zone', tweet.user.time_zone),
                 ('id_user', idUser),
                 ('username', username),
                 ('text' , text)
             ])

            with open('tweetDB-AU-15-Oct.json', 'a') as f:
                json.dump(data,f, sort_keys=False)
                f.write('\n')

        except BaseException, e:
            print 'failed', str(e)
            time.sleep(5)

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

# Bounding boxes for geolocations
# Online-Tool to create boxes (c+p as raw CSV): http://boundingbox.klokantech.com/
# Western Australia GeoBox  112.92,-35.19,129.0,-13.69
sapi = tweepy.streaming.Stream(auth, CustomStreamListener())
sapi.filter(locations= locs,languages=['en'], async=False)
