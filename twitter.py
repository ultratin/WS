import tweepy
from pymongo import MongoClient
import json
import queue
import threading
import time
from datetime import datetime
import sys
import spacy
import string
import re
# Config files contain passwords and api keys
import config
#requires python download
nlp = spacy.load('en')

#API KEYS
consumer_token = config.consumer_token
consumer_secret = config.consumer_secret
access_token = config.access_token
access_secret = config.access_secret

#Bounding box coords from http://boundingbox.klokantech.com/
SINGAPORE = [103.607933,1.222229,104.031594,1.466607]
#SG Long Lat and radius
SG_GEO = ("1.3521,103.8198,25km")

#Mongo DB Information
client = MongoClient('localhost', 27017)
db = client['twitter2']  
streaming_collection = db['streaming']
rest_collection = db['rest']

# Global Variables
stream_process = queue.Queue()
ner_existed = set([])
trends_existed = set([])
users_existed = set([])
querying_queue = queue.Queue()
is_since = False
since_id_query = 0

# STREAM api class and puts data in queue so purely listening for data
class MyStreamListener(tweepy.StreamListener):

    def on_data(self, data):
        stream_process.put(data)
        return True

# Stream function for listening to streaming API, filtering only singapore based on bounding box
def stream_api(auth, listener, location):
    print("Stream thread {} working".format(threading.current_thread()))
    streaming = tweepy.Stream(auth, listener)
    streaming.filter(locations = location)

# Importing to mongodb for STREAMING API only
def mongo_import():
    print("Import thread {} working".format(threading.current_thread()))
    global is_since
    global since_id_query
    while(1):
        if stream_process.empty():
            pass
        data = stream_process.get()
        json_data = json.loads(data)
         
       
        if json_data['lang'] == 'en':
             # check if place is SG as bounding box include some area of malaysia and indonesia
            # if place is None, assume it is from singapore and accept
            if json_data['place']is None or json_data['place']['country_code'] == 'SG':
                # Save the first twitter json id for since_id field in REST api
                if is_since == False:
                    since_id_query = json_data['id']
                    is_since = True
                # Insert into mongodb streaming collectiojn
                streaming_collection.insert(json_data)

                # NER with space. process tweets first by removing links, emojis and whitespace (\n, \t)
                text = text_processing(json_data['text'])
                doc = nlp(text)

                # For every ent recognized, but them in a querying queue for rest api and put them in a set.
                # if existed in the set, don't add in anymore to prevent re-querying
                for ent in doc.ents:
                    if ent.text not in ner_existed:
                        ner_existed.add(ent.text)
                        querying_queue.put(ent.text)

# Text processing for NER, did not lowercase because spacy use cases as entity recognition
def text_processing(text):
    text = re.sub(r"http\S+", "", text) # Remove links
    text = ''.join(filter(lambda x: x in string.printable, text)) # Remove non ascii (Emoji)
    text = re.sub('\s+',' ',text) # Remove whitespace
    return text


# REST API for twitter
def rest_api(auth, api , start_time):
    print("Rest thread {} working".format(threading.current_thread()))
    # Infinite loop of querying based on NER, current Trends and most retweeted users
    while(1):
        #Query NER gotten from streaming api
        while querying_queue.qsize() > 0:
            ner_string = querying_queue.get()
            print(f"NER: {ner_string}")
            rest_insert(api, ner_string,start_time)
        
        #Get the current trends and process it for querying format
        raw_trends = api.trends_place(23424948)
        trend_list = trends_process(raw_trends)
        for trends in trend_list:
            trend_string = trends_list_to_string(trends)
            print(f"Trends: {trend_string}")
            rest_insert(api, trend_string, start_time)
        
        # Get the top users based on currently collected data and query those users
        user_list = top_retweets()
        for users in user_list:
            user_string = user_list_to_string(users)
            print(f"User: {user_string}")
            rest_insert(api, user_string, start_time)
        
        print("Rest sleeping")
        time.sleep(300)

# User for inserting into mongodb
def rest_insert(api, query, start_time):
    # Querying based on SG, since_id from the first tweet gotten from streaming and querying field.
    search = tweepy.Cursor(api.search, q = query , geocode = SG_GEO, count = 1000, since_id = since_id_query, lang='en')
    for tweets in search.items():
        # Double check for that the data that is gotten is after the start time of this process
        data = tweets._json
        data_time = datetime.strptime(data["created_at"],'%a %b %d %H:%M:%S +0000 %Y')
        #Check if tweet gotten is after program start time and is in english
        if data_time > start_time :
            # Check if the the tweet is from SG, if NULL assume from SG
            if data['place']is None :
                rest_collection.insert(tweets._json)
            elif data['place']['country_code'] == 'SG':
                rest_collection.insert(tweets._json)
            else:
                pass

# Return a string for trends in querying format
def trends_list_to_string(trends_list):
    name = ' OR '.join(trends_list)
    return name  

# Return a string for user in querying format
def user_list_to_string(user_list):
    name = 'from:' + ' OR from:'.join(user_list)
    return name

# Get the users with the higest amount of retweets
def top_retweets():
    retweets = list(rest_collection.find({"retweeted_status":{"$ne": None}}))
    retweets.sort(key = retweet_counts, reverse = True)

    users_list = []
    for tweets in retweets:
        user = tweets['retweeted_status']['user']['screen_name']
        #If the user has not been queried before, put in a set to ensure that it won't be requeried  
        if user not in users_list and user not in users_existed:
            users_list.append(user)
            users_existed.add(user)
    
    return list(chunks(users_list, 10))

# Format the top 10 trends into a list
def trend_name_list(trends):
    name = []
    for items in trends:
        trends = items['query']
        # If the trends has not been queried before, add into set so it won't be requeried
        if trends not in trends_existed:
            name.append(trends)
            trends_existed.add(trends)
    name = list(chunks(name, 10))
    return name

# Return a list for retweet count
def retweet_counts(list):
    try:
        return int(list['retweet_count'])
    except KeyError:
        return 0
 
# User for above function
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


# process the raw trends as it is given in a json format
# Rank the trends accordingly to their tweet volume
def trends_process(raw_trends):
    trends = raw_trends[0]['trends']
    if len(trends) < 10:
        return trend_name_list(trends)
    else:
        ranking = sorted(trends, key=lambda items: int(0) if items['tweet_volume'] is None else items['tweet_volume'], reverse = True)
        return trend_name_list(ranking)


# Main function
if __name__ == '__main__':
    # Set up for streaming listener and the API using tweepy
    auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
    auth.set_access_token(access_token,access_secret)
    # Set up REST and Streaming API. Wait on rate limit will restart REST API automatically once it hits API limit.
    api = tweepy.API(auth, wait_on_rate_limit= True, wait_on_rate_limit_notify= True)
    listener = MyStreamListener()

    streaming_collection.drop()
    rest_collection.drop()

    # Start time for reference for REST API
    start_time = datetime.utcnow()
    print(f"Starting Time: {start_time}")
    try:
        # Set up threads for Streaming, Streaming import and REST
        t1 = threading.Thread(target = stream_api, args= (auth, listener, SINGAPORE,))
        t2 = threading.Thread(target= mongo_import)
        
        t1.daemon = True
        t2.daemon = True
        
        # Start streaming API
        print("Starting Streaming")
        t1.start() 
        t2.start()
        # Sleep for 5 minutes before starting REST API to get enough NER
        time.sleep(300)
        
        # After sleeping for 5 minutes start rest api
        print("Starting REST")
        t3 = threading.Thread(target= rest_api, args = (auth, api, start_time, ))
        t3.daemon = True
        t3.start()
        # Main threads sleep for 55 minutes for a total of 1 hour and stops the program
        time.sleep(3180)
        print("Exiting")
        sys.exit(0)
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)