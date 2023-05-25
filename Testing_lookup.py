from pymongo import MongoClient
from bson.objectid import ObjectId

# Connect to the MongoDB server running on localhost
client = MongoClient('mongodb://localhost:27017/')

# Access the database
db = client['Skittles_DB']

# Access the collection
collection = db['dst_listing']

dst_file_dir_collection = db["dst_file_dir_relations"]




aggregation = [
    {
        '$match': {
            'dir_ID': ObjectId('646ec668d7444973234fdb82')
        }
    }, {
        '$lookup': {
            'from': 'dst_listing', 
            'localField': 'file_ID', 
            'foreignField': '_id', 
            'as': 'result'
        }
    }, {
        '$project': {
            'filesize': {
                '$arrayElemAt': [
                    '$result.filesize', 0
                ]
            }
        }
    }, {
        '$group': {
            '_id': None, 
            'totalSize': {
                '$sum': '$filesize'
            }
        }
    }
]



results = dst_file_dir_collection.aggregate(aggregation)
for result in results:
    print(result)

