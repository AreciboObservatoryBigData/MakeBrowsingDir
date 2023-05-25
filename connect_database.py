from pymongo import MongoClient

def connect_to_mongodb_listing():
    # Connect to the MongoDB server running on localhost
    client = MongoClient('mongodb://localhost:27017/')

    # Access the database
    db = client['Skittles_DB']

    # Access the collection
    collection = db['dst_listing']

    return collection


def connect_to_mongodb_relation():
    # Connect to the MongoDB server running on localhost
    client = MongoClient('mongodb://localhost:27017/')

    # Access the database
    db = client['Skittles_DB']

    # Access the collection
    relation = db['dst_file_dir_relations']

    return relation