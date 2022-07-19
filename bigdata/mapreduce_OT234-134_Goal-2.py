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

def count_words_answers(data):
    try:
        AnswerCount = int(data.attrib['AnswerCount'])
    except:
        return None
    
    body = data.attrib['Body']
    body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))',body)
    words_count = len(body)
    
    return AnswerCount, words_count

def mapper(data):
    mapped_count = list(map(count_words_answers, data))
    mapped_count = list(filter(None, mapped_count))
   
    return mapped_count

def calculate_ratio(data):
    try:
        Ratio = data[1] / data[0]
    except:
        Ratio = 'N/A'
    return Ratio
    

##########################################################################3
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

# Calculate words/answers ratio:
flat_mapped = [item for sublist in mapped for item in sublist]  # Flatten list:
Ratio = list(map(calculate_ratio, flat_mapped))

## Print results:
clear = "\n" * 5
print(clear)
print("Ratio between number of words in post and number of answers (words/answers): ")
print("N/A Stands for no answers: ")
print(Ratio)