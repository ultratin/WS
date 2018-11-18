import json

from pymongo import MongoClient

#Mongo DB Information
client = MongoClient('localhost', 27017)
db = client['reddit']  
reddit_collection = db['reddit_collection']
# Please use the below code if you are running on the 5 min sample and comment the one right above this
# reddit_collection = db["reddit_submission"]


#Process count for hot and new individually
def process_upvotes_comments(source):
    total_upvotes       = 0
    total_comments      = 0
    highest_upvotes     = 0
    highest_comments    = 0
    # streaming db
    for doc in reddit_collection.find({'source':source}):
        # count most upvotes and total upvotes
        total_upvotes += int(doc['score'])
        if highest_upvotes < int(doc['score']):
            highest_upvotes = int(doc['score'])
        # count post with highest comments and total comments
        total_comments += int(doc['num_comments'])
        if highest_comments < int(doc['num_comments']):
            highest_comments = int(doc['num_comments'])
    
    print(f"\nUpvotes Count for {source}")
    print(f"highest upvotes: {highest_upvotes}")
    print(f"total upvotes: {total_upvotes}")
    print(f"Comments count for {source} ")
    print(f"highest comments: {highest_comments}")
    print(f"total comments: {total_comments}")


if __name__ == '__main__':
    
    # total post counting
    rest_hot_count = reddit_collection.find({'source':'hot'}).count()
    rest_new_count = reddit_collection.find({'source':'new'}).count()
    stream_count = reddit_collection.find({'source':'stream'}).count()
    total_count = rest_hot_count + rest_new_count + stream_count

    print("counts: ")
    print(f"hot api count: {rest_hot_count}")
    print(f"new api count: {rest_new_count}")
    print(f"stream api count: {stream_count}")
    print(f"total posts count: {total_count}")

    process_upvotes_comments('new')
    process_upvotes_comments('hot')
    process_upvotes_comments('stream')

    # Counting how much post from streaming and new made it into hot
    hot_list = list(reddit_collection.find({"source": "hot"}))
    stream_list = list(reddit_collection.find({"source" : "stream"}))
    new_list = list(reddit_collection.find({"source" : "new"}))
    hot_existed = set([])
    for post in hot_list:
        hot_existed.add(post['id'])
    
    hot_stream_count = 0
    for post in stream_list:
        if post['id'] in hot_existed:
            hot_stream_count += 1

    hot_new_count = 0
    for post in new_list:
        if post['id'] in hot_existed:
            hot_new_count += 1

    print(f"number of streaming posts that got into hot during the 1 hour: {hot_stream_count}")
    print(f"number of new posts that got into hot during the 1 hour: {hot_new_count}" )
