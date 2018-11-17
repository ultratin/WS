import time
import praw
import json 
import multiprocessing
import config
import sys

from pymongo import MongoClient
from datetime import datetime

#Mongo DB Information
client = MongoClient('localhost', 27017)
db = client['reddit']  
reddit_collection = db['reddit_collection']

# List of variables to get in a single submission, this is because there is an infinite loop in the query if there is no processing, 
# so it is limited
fields = ('id','title', 'url', 'score', 'created_utc',
        'num_comments', 'selftext', 'author', 'subreddit' )

# consumer for taking from queue
def mongo_import(queue):
    while True:
        data = queue.get()
        reddit_collection.insert(data)

# Streaming reddit API
def stream_reddit(subs, queue):

    # set up reddit api for streaming
    reddit = praw.Reddit(client_id      =config.reddit_id,
                        client_secret   =config.reddit_secret,
                        password        =config.reddit_password, 
                        user_agent      =config.reddit_user_agent,
                        username        =config.reddit_username)

    # Get streaming post from top 125 subs
    for submission in reddit.subreddit(subs).stream.submissions():
        to_dict = vars(submission)
        sub_dict = {field:str(to_dict[field]) for field in fields}
        sub_dict['source'] = 'stream'
        queue.put(sub_dict)

# REST reddit API
def rest_reddit(api, source, list_of_subs):
    
    for sub in list_of_subs:
        subreddit = api.subreddit(sub)
        
        # Limit the top 100 post accordingly to function query for hot or new post
        if source == "hot":
            submissions = subreddit.hot(limit=100)
        else:
            submissions = subreddit.new(limit=100)
        
        #insert into reddit collections
        for submission in submissions:
            to_dict = vars(submission)
            sub_dict = {field:str(to_dict[field]) for field in fields}
            sub_dict['source'] = source
            reddit_collection.insert(sub_dict)

    print("completed rest", source)

# for killing a process
def kill_process(p):
    p.terminate()
    print(p.name, "process killed")
    p.join()

# Main method
if __name__ == '__main__':

    # reddit connection
    api = praw.Reddit(client_id         =config.reddit_id,
                        client_secret   =config.reddit_secret,
                        password        =config.reddit_password, 
                        user_agent      =config.reddit_user_agent,
                        username        =config.reddit_username)

    # Get top 125 reddits, the json is gotten from http://redditlist.com/, and converted into json format
    with open('top125_subreddit.json') as fd:
        top_subreddits = json.load(fd)

    reddit_subs = []
    for sub in top_subreddits:
        reddit_subs.append(sub['subreddit'])
        
    # Process subreddits for stream api
    subreddits = "+".join(reddit_subs)

    queue = multiprocessing.Queue()
    
    # for holding the processes
    jobs = []
    # Start time for reference for REST API
    start_time = datetime.utcnow()
    print(f"Starting Time: {start_time}")
    try:
        #start streaming process
        stream_p = multiprocessing.Process(target=stream_reddit, args=(subreddits,queue))
        jobs.append(stream_p)
        stream_p.start()

        # start hot and new rest api process
        rest_hot_p = multiprocessing.Process(target=rest_reddit, args=(api,"hot",  reddit_subs))
        rest_new_p = multiprocessing.Process(target=rest_reddit, args=(api,"new",  reddit_subs))
        jobs.append(rest_hot_p)
        jobs.append(rest_new_p)
        rest_hot_p.start()
        rest_new_p.start()
        
        # queue processor for writing to db
        queue_p = multiprocessing.Process(target=mongo_import, args=(queue,))
        jobs.append(queue_p)
        queue_p.start()

        # Sleep for one hour and exit on wake
        time.sleep(3600)
        print("Exiting")
        for p in jobs:
            kill_process(p)
        sys.exit(0)
    except KeyboardInterrupt:
        for p in jobs:
            kill_process(p)
        sys.exit(0)
