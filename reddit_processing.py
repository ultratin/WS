import json

from pymongo import MongoClient

# db collection
reddit_db = 'reddit4'

def process_upvotes_comments(source, db):
    upvotes = 0
    comments = 0
    highest_upvotes = 0
    highest_comments = 0
    # streaming db
    for doc in db.find({'source':'stream'}):
        # upvotes processing
        upvotes += int(doc['score'])
        if highest_upvotes < int(doc['score']):
            highest_upvotes = int(doc['score'])
        # comments processing
        comments += int(doc['num_comments'])
        if highest_comments < int(doc['num_comments']):
            highest_comments = int(doc['num_comments'])
    
    print("\n-----------", source, "-----------")
    print("Upvotes: ")
    print("highest upvotes:", highest_upvotes)
    print("total upvotes:", upvotes)
    print("Comments: ")
    print("highest comments:", highest_comments)
    print("total comments:", comments)


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db = client['ws'][reddit_db]
    
    
    # total post counting
    rest_hot_count = db.find({'source':'hot'}).count()
    rest_new_count = db.find({'source':'new'}).count()
    stream_count = db.find({'source':'stream'}).count()
    total_count = rest_hot_count + rest_new_count + stream_count

    print("\nResults from the previous reddit crawler run: \n----------------------")
    print("counts: ")
    print("hot api count:", rest_hot_count)
    print("new api count:", rest_new_count)
    print("stream api count:", stream_count)
    print("total posts count:", total_count)

    process_upvotes_comments('new', db)
    process_upvotes_comments('hot', db)
    process_upvotes_comments('stream', db)

  
    # # checking if new submissions got into hot
    # duplicates_rest = 0
    # duplicates_stream = 0

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

    for result in db.aggregate(hot_stream_pipeline):
        print("number of streaming posts that got into hot during the 1 hour:", result)
    
    
    for result in db.aggregate(hot_new_pipeline):
        print("number of new posts that got into hot during the 1 hour:", result)
