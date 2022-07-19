import xml.etree.ElementTree as ET
from typing import Counter
from functools import reduce
import re
import os

##### Map reduce script for Stack Overflow database analysis
## Goals:
# 1-Top 10 type of post with highest accepted answers
    # Criteria: get the tags of only the accepted answers and count them
    # Then obtain the top 10 tags to get the type of post
# 2-Ratio between number of words in post and number of answers
# 3-Top ten users with highest percentage of favorite answers


def chunckify(iterable,len_of_chunk):
    for i in range(0,len(iterable), len_of_chunk):
        yield iterable[i:i + len_of_chunk]

def get_User_FavoriteCount(data):
    try:  # only gets results if 'OwnerUserID' and 'FavoriteCount' exists
        OwnerUserID = data.attrib['OwnerUserId']
        FavoriteCount = data.attrib['FavoriteCount']
    except:
        return None
   
    UserID = int(OwnerUserID) # reads ID from attribute
    Favorite = int(FavoriteCount) # reads Favorite Count

    return UserID, Favorite

def mapped_dict(data):
    UserID = data[0]
    Favorite = data[1]
    user_dict = {UserID: Counter({UserID : Favorite})}
    
    return user_dict

def reduce_users(data1, data2):
    for key, value in data2.items():
        if key in data1.keys():
            data1[key].update(data2[key])
        else:
            data1.update({key: value}) 

    return data1 

def mapper(data):
    mapped = list(map(get_User_FavoriteCount, data))
    mapped = list(filter(None, mapped))
    dict_mapped = map(mapped_dict, mapped)

    try:
        reduced = reduce(reduce_users, dict_mapped)
    except:
        return None
 
    return reduced
 
def calculate_top_10(data):
    return data.most_common(10)
    
##########################################################################

## Data reading
file_path = os.path.dirname(os.path.abspath(__file__))
datafile = file_path + '/datasets/112010 Meta Stack Overflow/posts.xml'

## xml parser
tree = ET.parse(datafile)
root = tree.getroot()

# Separates data in chunks 
data_chunks = chunckify(root, 50)

# Data mapping:
mapped = list(map(mapper, data_chunks))
mapped = list(filter(None, mapped))
reduced = reduce(reduce_users, mapped)

#Calculate Top 10 tags
reduced_cleaned = dict([[key, list(reduced[key].values())[0]] for key in reduced.keys()])
reduced_cleaned = Counter(reduced_cleaned)
top_10 = calculate_top_10(reduced_cleaned)

## Print results (Terminal):
clear = "\n" * 5
print(clear)
print("Top ten users with highest of favorite answers: ")
print(top_10)

