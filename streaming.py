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
import config
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize
from nltk import pos_tag
#requires python download
nlp = spacy.load('en')
stop_words = stopwords.words("english")
wnl = WordNetLemmatizer()

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
db = client['twitter']  
streaming_collection = db['streaming']
rest_collection = db['rest']

stream_process = queue.Queue()
ner_existed = set([])
trends_existed = set([])
users_existed = set([])
querying_queue = queue.Queue()
is_since = False
since_id_query = 0


class MyStreamListener(tweepy.StreamListener):

    def on_data(self, data):
        stream_process.put(data)
        return True

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
            if json_data['place']is None or json_data['place']['country_code'] == 'SG':
                if is_since == False:
                    since_id_query = json_data['id']
                    print(f"Since ID: {since_id_query}")
                    is_since = True
                streaming_collection.insert(json_data)
                text = text_processing(json_data['text'])
                # text = json_data['text']
                doc = nlp(text)
                for ent in doc.ents:
                    if ent.text not in ner_existed:
                        ner_existed.add(ent.text)
                        querying_queue.put(ent.text)
            else:
                print("not imported: country: {}".format(json_data['place']['country_code']))

def text_processing(text):
    translation = str.maketrans('','', string.punctuation)
    text = text.lower() # Lowercase everything
    text = text.translate(translation) # Remove Puntuations
    text = ''.join(filter(lambda x: x in string.printable, text)) #remove non ascii (Emoji)
    text = text.rstrip() # REmove whitespace
    text = re.sub(r"http\S+", "", text) # remove links
    text = word_tokenize(text)
    text = ([word for word in text if word not in stop_words])
    text = [wnl.lemmatize(t, pos = 'n') for t in text]

    
    
    return text

def stream_api(auth, listener, location):
    print("Stream thread {} working".format(threading.current_thread()))
    streaming = tweepy.Stream(auth, listener)
    streaming.filter(locations = location)


def rest_api(auth, api , start_time):
    print("Rest thread {} working".format(threading.current_thread()))
    while(1):
        while querying_queue.qsize() > 0:
            ner_string = querying_queue.get()
            print(f"NER: {ner_string}")
            rest_insert(api, ner_string,start_time)
        
        raw_trends = api.trends_place(23424948)
        trend_list = trends_process(raw_trends)
        for trends in trend_list:
            trend_string = trends_list_to_string(trends)
            print(f"Trends: {trend_string}")
            rest_insert(api, trend_string, start_time)
        
        # user_list = top_retweets()
        # for users in user_list:
        #     user_string = user_list_to_string(users)
        #     print(f"User: {user_string}")
        #     rest_insert(api, user_string, start_time)

def rest_insert(api, query, start_time):
    search = tweepy.Cursor(api.search, q = query , geocode = SG_GEO, count = 1000, since_id = since_id_query)
    
    for tweets in search.items():
        data = tweets._json
        data_time = datetime.strptime(data["created_at"],'%a %b %d %H:%M:%S +0000 %Y')
        if data_time > start_time :
            if data['place']is None:
                rest_collection.insert(tweets._json)
            elif data['place']['country_code'] == 'SG':
                rest_collection.insert(tweets._json)
            else:
                pass
# return list for trends
def trends_list_to_string(trends_list):
    name = ' OR '.join(trends_list)
    return name  

def user_list_to_string(user_list):
    name = 'from:' + ' OR from:'.join(user_list)
    return name

def top_retweets():
    retweets = list(rest_collection.find({"retweeted_status":{"$ne": None}}))
    retweets.sort(key = retweet_counts, reverse = True)

    users_list = []
    for tweets in retweets:
        user = tweets['retweeted_status']['user']['screen_name']
        if user not in users_list and user not in users_existed:
            users_list.append(user)
            users_existed.add(user)
    
    return list(chunks(users_list, 10))

def retweet_counts(list):
    try:
        return int(list['retweet_count'])
    except KeyError:
        return 0

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

# Format the top 10 trends into a string
def trend_name_list(trends):
    name = []
    for items in trends:
        trends = items['query']
        if trends not in trends_existed:
            name.append(trends)
            trends_existed.add(trends)
    name = list(chunks(name, 10))
    return name

# return the top 10 trends for rest query
def trends_process(raw_trends):
    trends = raw_trends[0]['trends']
    if len(trends) < 10:
        return trend_name_list(trends)
    else:
        ranking = sorted(trends, key=lambda items: int(0) if items['tweet_volume'] is None else items['tweet_volume'], reverse = True)
        return trend_name_list(ranking)



if __name__ == '__main__':
    listener = MyStreamListener()
    auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
    auth.set_access_token(access_token,access_secret)
    api = tweepy.API(auth, wait_on_rate_limit= True, wait_on_rate_limit_notify= True)

    streaming_collection.drop()
    rest_collection.drop()
    start_time = datetime.utcnow()
    print(f"Starting Time: {start_time}")
    try:
        t1 = threading.Thread(target = stream_api, args= (auth, listener, SINGAPORE,))
        t2 = threading.Thread(target=mongo_import)
        
        t1.daemon = True
        t2.daemon = True
        
        print("Starting Streaming")
        t1.start() 
        t2.start()
        time.sleep(300)
        
        print("Starting REST")
        t3 = threading.Thread(target= rest_api, args = (auth, api, start_time, ))
        t3.daemon = True
        t3.start()
        time.sleep(3300)
        print("Exiting")
        sys.exit(0)
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)