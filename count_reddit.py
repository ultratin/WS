import json

from pymongo import MongoClient

#Mongo DB Information
client = MongoClient('localhost', 27017)
db = client['reddit']  
reddit_collection = db['reddit_collection']


#Process count for hot and new individually
def process_upvotes_comments(source):
    upvotes = 0
    comments = 0
    highest_upvotes = 0
    highest_comments = 0
    # streaming db
    for doc in reddit_collection.find({'source':source}):
        # upvotes processing
        upvotes += int(doc['score'])
        if highest_upvotes < int(doc['score']):
            highest_upvotes = int(doc['score'])
        # comments processing
        comments += int(doc['num_comments'])
        if highest_comments < int(doc['num_comments']):
            highest_comments = int(doc['num_comments'])
    
    print(f"\nUpvotes Count for {source}")
    print(f"highest upvotes: {highest_upvotes}")
    print(f"total upvotes: {upvotes}")
    print(f"Comments count for {source} ")
    print(f"highest comments: {highest_comments}")
    print(f"total comments: {comments}")


if __name__ == '__main__':
    
    # total post counting
    rest_hot_count = reddit_collection.find({'source':'hot'}).count()
    rest_new_count = reddit_collection.find({'source':'new'}).count()
    stream_count = reddit_collection.find({'source':'stream'}).count()
    total_count = rest_hot_count + rest_new_count + stream_count

    print("\nResults from the previous reddit crawler run: \n----------------------")
    print("counts: ")
    print(f"hot api count: {rest_hot_count}")
    print(f"new api count: {rest_new_count}")
    print(f"stream api count: {stream_count}")
    print(f"total posts count: {total_count}")

    process_upvotes_comments('new')
    process_upvotes_comments('hot')
    process_upvotes_comments('stream')

    # pipeline for aggregation in mongodb
    hot_stream_pipeline = [
        {"$match" : 
            {"$or" : [{"source":"hot"}, {"source":"stream"}]}},
        {"$group" : 
            {"_id": { "id" : "$id" },
            "uniqueIds": { "$addToSet": "$_id"},
            "count": {"$sum": 1} }},
        {"$match" : 
            {"count": {"$gt":1}}},
        {"$count" : "count"}
    ]
    
    hot_new_pipeline = [
        {"$match" : 
            {"$or" : [{"source":"hot"}, {"source":"new"}]}},
        {"$group" : 
            {"_id": { "id" : "$id" },
            "uniqueIds": { "$addToSet": "$_id"},
            "count": {"$sum": 1} }},
        {"$match" : 
            {"count": {"$gt":1}}},
        {"$count" : "count"}
    ]

    for result in reddit_collection.aggregate(hot_stream_pipeline):
        print(f"number of streaming posts that got into hot during the 1 hour: {result}")
    
    
    for result in reddit_collection.aggregate(hot_new_pipeline):
        print(f"number of new posts that got into hot during the 1 hour: {result}" )
