from pymongo import MongoClient
from connect_db import connect_to_mongodb_src
import os, glob, time
import pandas as pd

# connection to MongoDB
database_name = "Skittles_DB"
db=connect_to_mongodb_src(database_name)
dst_name = "dst_listing"
connection_to_dst = db["dst_listing"]


csv_path = 'CSV_path/'
file_list = glob.glob(os.path.join(csv_path , "*.csv"))

dataframes = []
for file in file_list:
    dataframe = pd.read_csv(file)
    dataframes.append(dataframe)
if len(dataframes) == 0:
    print("No csv found")

elif len(dataframes) == 1:
    df = dataframes[0]

else:
    concatenated_df = pd.concat(dataframes)
    concatenated_df = concatenated_df.reset_index(drop=True)
    df = concatenated_df

def main():
    folder_list = []
    star_list = []
    for index, row in df.iterrows():
        if row['path'][-1] == "/" :
            folder_list.append(row)
        else:
            star_list.append(row)

    folder_df = pd.DataFrame(folder_list)
    star_df = pd.DataFrame(star_list)
    key_function = lambda item: item.count("/")
    folders_df = folder_df.iloc[folder_df['path'].apply(key_function).argsort()]


    for index, row in folder_df.iterrows():
        criteria = row["path"]
        
        folder_match = criteria[:-1]
        criteria = f"{criteria}.*"

        query = {'$or': [
                {'filepath': folder_match},
                {'filepath': {'$regex': criteria}}
                ]}
        updateTags(query, row)



    for index, row in star_df.iterrows():
        criteria = row["path"]
        
        if "*" in criteria :
            dir_name = os.path.dirname(criteria)
            if criteria [-1] != "*":
                criteria=(f"{criteria}$")
            pattern = criteria.replace(".", "\.")
            pattern = criteria.replace("*", ".*")
            query =[{
                        '$match': {
                            'dir_name': dir_name,
                            'filetype': {'$not': {'$in': ['d', 'ld']}}
                        }
                    }, {
                        '$match': {'filename': {'$regex': pattern}}
                    }
                ]
        else:
            query = {
                "filepath": criteria
            }
        
        updateTags(query, row)

def updateTags(query, row):
    startTime = time.time()
    batch = []
    if type(query)==type({}):
        result = connection_to_dst.find(query)
    elif type(query)==type([]):
        result = connection_to_dst.aggregate(query)
    i = 0
    for item in result:
        mult_tags = row["tags"]
        tags_array = [word for word in mult_tags.split() if word]
        item ["tags"] = tags_array
        batch.append(item)
        i = i + 1
        if i % 250000 == 0:
            for doc in batch:
                filter = {"_id": doc["_id"]}
                connection_to_dst.replace_one(filter, doc, upsert=True)
            batch = []
            finishTime=time.time()
            print(finishTime-startTime)
    if batch != []:
        for doc in batch:
            filter = {"_id": doc["_id"]}
            connection_to_dst.replace_one(filter, doc, upsert=True)
        batch = []



main()