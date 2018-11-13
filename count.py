from pymongo import MongoClient
import numpy as np

#Mongo DB Information
client = MongoClient('localhost', 27017)
db = client['twitter']
streaming_collection = db['streaming']
rest_collection = db['rest']


if __name__ == '__main__':
    rest_total = rest_collection.count()
    streaming_total = streaming_collection.count()
    total_count =  rest_total + streaming_total
    no_geo = rest_collection.count_documents({'place': {'$type': 10}})
    geo_tagged = total_count - no_geo
    sg_geo = rest_collection.find({"place.country_code": "SG"}).count() + streaming_total

    rest_quoted = rest_collection.find({"is_quote_status": True}).count()
    streaming_quoted = streaming_collection.find({"is_quote_status": True}).count()

    rest_retweeted = rest_collection.find({"retweeted_status" : {'$ne' : None}}).count()
    streaming_retweeted = streaming_collection.find({"retweeted_status" : {'$ne' : None}}).count()

    streaming_id = np.array(
        list(map(lambda x: x['id'], (streaming_collection.find({}, {'id': 1, '_id':0})))
        ))

    rest_id = np.array(
        list(map(lambda x: x['id'], (rest_collection.find({}, {'id': 1, '_id': 0})))))

    overall_id = np.append(streaming_id,rest_id)
    unique_data = np.unique(overall_id)
    overlap = len(np.intersect1d(streaming_id,rest_id))
    print(f"Total: {total_count}")
    print(f"Geo tagged: {geo_tagged}")
    print(f"SG Geo: {sg_geo}")
    print(f"Intersect: {overlap}")
    print(f"Redundant : {total_count - len(unique_data)}")
    print(f"Quoted : {rest_quoted + streaming_quoted}")
    print(f"Retweeted : {rest_retweeted + streaming_retweeted}")