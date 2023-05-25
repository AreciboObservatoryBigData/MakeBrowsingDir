from pymongo import MongoClient
from connect_database import connect_to_mongodb_listing
from connect_database import connect_to_mongodb_relation
import glob
import os
from bson.objectid import ObjectId
import time
import multiprocessing as mp

folders_path = "test/folders_test"
reset_dir = True


def main():

    global collection
    global relation

    # Connect to MongoDB
    collection =  connect_to_mongodb_listing()
    relation = connect_to_mongodb_relation()



    # Query documents where filetype is "d"
    # query = {'filetype': 'd'}
    test_query = {
        'filepath': '/stornext/ranch_103/ranch/projects/Arecibo-Observatory/Legacy/Sciences/Atmospheric-Sciences/T1193'
        }
    query = test_query
    results = collection.find(query)

    if reset_dir == True:
        command="rm -r " + folders_path
        os.system (command)

    command = "mkdir " + folders_path
    os.system(command)

    for result in results:
        
        line=result['filepath']
        elements = line.split("/")[6:]
        new_output_dir_path = "/".join(elements)
        new_output_dir_path = os.path.join(folders_path, new_output_dir_path)
        if not os.path.exists(new_output_dir_path):
            command = "mkdir -p '" + new_output_dir_path + "'"
            os.system(command)
        

        start_time = time.time()
        total_size=0
        # Add recursive size
        # Get all file Ids in dst_file_dir where dir_ID == result["_id"]
        id=result['_id']
        recursive = relation.find({'dir_ID':ObjectId(id)})
        i=0
        print(f"Proj _Id: {id}")
        total_size = 0
        arguments = []
        for_loop_time = time.time()
        for x in recursive:
            file=x['file_ID']
            # Example
            # size_start_time = time.time()
            # size = getSize(file)  
            # print("Size took :" + str(time.time()-size_start_time))
            # total_size = total_size + size 
            arguments.append(file)
            i=i+1
        print(time.time() - for_loop_time)
        print(f"Count:  {i}")
        # submit to multiprocessing
        pool = mp.Pool(100)
        multi_time = time.time()
        print("Tasks submitted to multiprocessing")
        results = pool.map(getSize, arguments)
        print(f"Multiprocessing finished {time.time()-multi_time}")
        pool.close()
        pool.join()
        total_size =  0
        for x in results:
            total_size = total_size + x
    
        print(f"Total size: {total_size}")
        
        elapsed_time = time.time() - start_time
        print("Elapsed time:", elapsed_time, "seconds\n\n")

# function should recieve the file_ID and return the filesize of that file
def getSize(id):
    # start_time = time.time()
    selected = collection.find_one({'_id':ObjectId(id)}, {"filesize": 1})
    size=selected['filesize']
    # print(time.time() - start_time)
    return size






main()