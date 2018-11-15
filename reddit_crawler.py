
import time
import praw
import json 
import multiprocessing
import config

from pymongo import MongoClient

# db collection
reddit_db = 'reddit4'

# list of things to get from a single submission
fields = ('id','title', 'url', 'score', 'created_utc',
        'num_comments', 'selftext', 'author', 'subreddit' )

stream_count = 0

def delete_db_content():
    # mongodb connection
    client = MongoClient('localhost', 27017)
    db = client['ws']
    db_col = db[reddit_db]

    # clear db content
    deleted_from_db = db_col.delete_many({})
    print("deleted from db:", deleted_from_db.deleted_count)

    # close db connection
    client.close()


# consumer for taking from queue
def removeFromQueue(queue):
    
    # mongodb connection
    client = MongoClient('localhost', 27017)
    db = client['ws']

    removed_count = 0
    try:
        while True:
            data = queue.get()
            db[reddit_db].insert(data)
            
            removed_count += 1
            if(removed_count % 200 == 0):
                print("inserted", removed_count , "into database")
    except KeyboardInterrupt:
        print("interrupted while trying to insert queue data")


# stream function
def stream_reddit(subs, queue):
    global stream_count

    reddit = praw.Reddit(client_id=config.reddit_id,
                        client_secret=config.reddit_secret,
                        password=config.reddit_password, 
                        user_agent=config.reddit_user_agent,
                        username=config.reddit_username)

    for submission in reddit.subreddit(subs).stream.submissions():
        stream_count += 1

        to_dict = vars(submission)
        sub_dict = {field:str(to_dict[field]) for field in fields}
        sub_dict['source'] = 'stream'
        queue.put(sub_dict)

        if stream_count % 50 == 0:
            print("stream count: ", str(stream_count))
    
# for killing a process
def kill_process(p):
    p.terminate()
    print(p.name, "process killed")
    p.join()

# for running a rest api
def rest_reddit(reddit, source, queue, list_of_subs):
    
    for sub in list_of_subs:
        subreddit = reddit.subreddit(sub)
        count = 0
        if source == "hot":
            # get the top 100 hot submissions from the subreddit
            submissions = subreddit.hot(limit=100)
        else:
            submissions = subreddit.new(limit=100)
            
        for submission in submissions:
            to_dict = vars(submission)
            sub_dict = {field:str(to_dict[field]) for field in fields}
            sub_dict['source'] = source
            queue.put(sub_dict)
            count += 1

        print("completed '", sub, "' ", source ,". count: ", str(count), sep="")  

    print("completed rest", source)


# main method
if __name__ == '__main__':

    # reset db content
    delete_db_content()

    # reddit connection
    reddit = praw.Reddit(client_id=config.reddit_id,
                        client_secret=config.reddit_secret,
                        password=config.reddit_password, 
                        user_agent=config.reddit_user_agent,
                        username=config.reddit_username)

    # get list of top 125 subreddit
    with open('top125_subreddit.json') as f:
        top125_subreddit = json.load(f)

    list_of_subs_name = []
    for sub in top125_subreddit:
        list_of_subs_name.append(sub['subreddit'])

    # for holding the processes
    jobs = []
        
    # collate subreddits for streaming
    subreddits = "+".join(list_of_subs_name)

    queue = multiprocessing.Queue()
    
    # try to start a stream process in the background
    try:
        #start streaming process
        stream_p = multiprocessing.Process(target=stream_reddit, args=(subreddits,queue))
        jobs.append(stream_p)
        stream_p.start()
        # start hot and new rest api process
        rest_hot_p = multiprocessing.Process(target=rest_reddit, args=(reddit,"hot", queue, list_of_subs_name))
        rest_new_p = multiprocessing.Process(target=rest_reddit, args=(reddit,"new", queue, list_of_subs_name))
        jobs.append(rest_hot_p)
        jobs.append(rest_new_p)
        rest_hot_p.start()
        rest_new_p.start()
        
        # queue processor for writing to db
        queue_p = multiprocessing.Process(target=removeFromQueue, args=(queue,))
        jobs.append(queue_p)
        queue_p.start()
        
        # sleep for 1 hr
        time.sleep(3600)
        
        # kill all processes
        for p in jobs:
            kill_process(p)

    except KeyboardInterrupt:
        print("main thread interrupted, stopping all threads")
        [kill_process(p) for p in jobs]
