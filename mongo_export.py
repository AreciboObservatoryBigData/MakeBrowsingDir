from pymongo import MongoClient
from connect_database import connect_to_mongodb_listing
from connect_database import connect_to_mongodb_relation
import os
import re
from bson.objectid import ObjectId
import time
import multiprocessing as mp

folders_path = "test/folders_test"
reset_dir = False


def main():

    global collection
    global relation


    # Connect to MongoDB
    collection =  connect_to_mongodb_listing()
    relation = connect_to_mongodb_relation()


    # Query documents where filetype is "d"
    query = {'filetype': 'd'}
    # test_query = {
    #     'filepath': '/stornext/ranch_103/ranch/projects/Arecibo-Observatory/Legacy/Sciences/Atmospheric-Sciences/T1193'
    #     }
    # query = test_query
    results = collection.find(query)

    if reset_dir == True:
        command="rm -r " + folders_path
        os.system (command)

    command = "mkdir " + folders_path
    os.system(command)


    start_time = time.time()
    dir_list=[]
    arguments = []
    db_dirs=[]
    i = 1
    for result in results:
        
        line=result['filepath']
        db_dirs.append(result['filepath'])
        elements = line.split("/")[6:]
        new_output_dir_path = "/".join(elements)
        new_output_dir_path = os.path.join(folders_path, new_output_dir_path)
        dir_list.append(new_output_dir_path)
        if not os.path.exists(new_output_dir_path):
            command = "mkdir -p '" + new_output_dir_path + "'"
            os.system(command)
        
        folder_path = line
        output_file = os.path.join(new_output_dir_path, "info.txt")
        info_dic = obtainNames(folder_path)
        with open (output_file,"w") as f:
            f.write("file_name  file_size   file_size_readable \n")

            for each in info_dic:
                size_readable = convertBytes(each['filesize'])
                f.write(f"{each['filename']}    {each['filesize']}   {size_readable} \n")
        breakpoint()

        arguments.append(result["_id"])
        if i % 10000 == 0:
            print(i)
            # break
        i += 1
    #implement multiprocess
    pool = mp.Pool(48)
    print("Submitting tasks")
    results = pool.map(multiprocessLookup, arguments)
    pool.close()
    pool.join()

    # for argument in arguments[100:]:
    #     print(argument)
    #     size = multiprocessLookup(argument)


    
    for z, result in enumerate(results):
        tb = convertBytes(result)
        txt_file = os.path.join(dir_list[z], "recursive_size.txt")
        with open(txt_file, 'w') as f:
            f.write(f"Recursive Size: {result} bytes, {tb} \n")
        total_time = time.time() - start_time
    print(total_time)
        

def multiprocessLookup(object_ID):
    # relation = connect_to_mongodb_relation()
    aggregation = [
            {
                '$match': {
                    'dir_ID': object_ID
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
    result = relation.aggregate(aggregation)
    result = [item for item in result]
    if len(result)==0:
        return_value = 0
    else:
        return_value = result[0]["totalSize"]


    return return_value



def convertBytes(byte_size):
    #Converts a byte size to MB, GB, and TB.
    sizes = ["B", "KB", "MB", "GB", "TB"]
    size_labels = ["Bytes", "KB", "MB", "GB", "TB"]
    base = 1000
    index = 0

    while byte_size >= base and index < len(sizes) - 1:
        byte_size /= base
        index += 1

    converted_size = round(byte_size, 2)
    tb = (f"{converted_size} {size_labels[index]}")
    return tb 

def obtainNames(folder_path):
    x=(f'^{folder_path}/.*')
    aggregation = [
            {
                '$project': {
                    'filename': 1, 
                    'filepath': 1, 
                    'filesize': 1, 
                    'filetype': 1
                }
            }, {
                '$match': {
                    'filepath': {
                        '$regex': x
                    }, 
                    'filetype': 'f'
                }
            }, {
                '$addFields': {
                    'splitString': {
                        '$split': [
                            '$filepath', '/'
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'array_len': {
                        '$size': '$splitString'
                    }
                }
            }, {
                '$match': {
                    'array_len': {
                        '$eq': 10 #change 10 for a value determined by each folder
                    }
                }
            }, {
                '$project': {
                    'filename': 1, 
                    'filesize': 1
                }
            }
        ]
    names = collection.aggregate(aggregation)
    names = [item for item in names]
    return names


main()