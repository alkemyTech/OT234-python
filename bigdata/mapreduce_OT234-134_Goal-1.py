import xml.etree.ElementTree as ET
from typing import Counter
from functools import reduce
import re
import os
import csv

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

def getAnswerTags(data):
    #get the tag for an accepted answer. If it does not exists returns None
    try:  # only gets results if 'tags' and 'AcceptedAnswerID' exists
        AcceptedAnswerID = data.attrib['AcceptedAnswerId']
        tags = data.attrib['Tags']
    except:
        return None
  
    tags = re.findall('<(.+?)>', tags) # reads tags from attribute
    tag_dict = dict([[tag, Counter([tag])] for tag in tags])
  
    return tag_dict # returns tags with counter value

def reduce_tags(data1, data2):
    for key, value in data2.items():
        if key in data1.keys():
            data1[key].update(data2[key])
        else:
            data1.update({key: value})
    
    return data1 

def mapper(data):
    mapped_answers = list(map(getAnswerTags, data))
    mapped_answers = list(filter(None, mapped_answers))
    try:
        reducido = reduce(reduce_tags, mapped_answers)
    except:
        return None
 
    return reducido
 
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
reduced = reduce(reduce_tags, mapped)

#Calculate Top 10 tags
reduced_cleaned = dict([[key, list(reduced[key].values())[0]] for key in reduced.keys()])
reduced_cleaned = Counter(reduced_cleaned)
top_10 = calculate_top_10(reduced_cleaned)

## Print results (Terminal):
clear = "\n" * 5
print(clear)
print("Top 10 type of post with highest accepted answers: ")
print(top_10)