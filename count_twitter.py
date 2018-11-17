from pymongo import MongoClient
import numpy as np

#Mongo DB Information
client = MongoClient('localhost', 27017)
db = client['twitter']
streaming_collection = db['streaming']
rest_collection = db['rest']

if __name__ == '__main__':
    # Get all total number of tweets from rest and streaming collection
    rest_total = rest_collection.count()
    streaming_total = streaming_collection.count()
    total_count =  rest_total + streaming_total

    # Get the total number of geo_tagged tweets by counting the non geo and subtracting it from the total count
    no_geo = rest_collection.count_documents({'place': {'$type': 10}})
    geo_tagged = total_count - no_geo

    # Get the total number of tweets that explicity states it is from SG
    sg_geo = rest_collection.find({"place.country_code": "SG"}).count() + streaming_collection.find({"place.country_code": "SG"}).count()

    # Get tweet id that intersect between streaming and REST API and count the intersect
    streaming_id = np.array(
        list(map(lambda x: x['id'], (streaming_collection.find({}, {'id': 1, '_id':0})))
        ))

    rest_id = np.array(
        list(map(lambda x: x['id'], (rest_collection.find({}, {'id': 1, '_id': 0})))))

    overall_id = np.append(streaming_id,rest_id)
    unique_data = np.unique(overall_id)
    overlap = len(np.intersect1d(streaming_id,rest_id))

    # Get the tweets that have been requeried by getting the total count subtracting with the number of unique tweets
    redundant = total_count - len(unique_data)

    # Get the number of quoted and retweet between REST and Streaming using the JSON field
    rest_quoted = rest_collection.find({"is_quote_status": True}).count()
    streaming_quoted = streaming_collection.find({"is_quote_status": True}).count()
    total_quoted = rest_quoted + streaming_quoted

    rest_retweeted = rest_collection.find({"retweeted_status" : {'$ne' : None}}).count()
    streaming_retweeted = streaming_collection.find({"retweeted_status" : {'$ne' : None}}).count()
    total_retweeted = rest_retweeted + streaming_retweeted
    
    # Print accordingly
    print(f"Total: {total_count}")
    print(f"Geo tagged: {geo_tagged}")
    print(f"SG Geo: {sg_geo}")
    print(f"Intersect: {overlap}")
    print(f"Redundant : {redundant}")
    print(f"Quoted : {total_quoted}")
    print(f"Retweeted : {total_retweeted}")