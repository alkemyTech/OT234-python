from typing import Counter
from functools import reduce
from lib.chunckify import chunckify
from lib.OT234_136_functions import get_duration
from lib.OT234_136_functions import get_dates, reducir_contadores, mapper_1
from lib.OT234_136_functions import get_answer_visit, get_post_type_1, get_ratio, mapper_2
from lib.OT234_136_functions import mapper_3, posts_reducer, reducer, get_answer_time
import xml.etree.ElementTree as ET
from pathlib  import Path
import copy

PARENT_PATH = Path(__file__).parent.absolute()

@get_duration
def map_reduce_1(data_chunks):
    mapped = map(mapper_1, data_chunks)
    counted = map(Counter, mapped)
    result_1 = reduce(reducir_contadores, counted).most_common(10)
    print(result_1)

@get_duration
def map_reduce_2(data_chunks):
    result_2 = list(map(mapper_2, data_chunks))
    print(result_2)

@get_duration
def map_reduce_3(data_chunks):
    dates_mapped = list(map(mapper_3, data_chunks))
    unchunk = list(reduce(reducer, dates_mapped))
    reduced = dict(reduce(posts_reducer, unchunk))
    duration_reduce = list(map(get_answer_time, reduced.values()))
    filtered = list(filter(None, duration_reduce))
    size  = len(filtered)
    result_3 = reduce(lambda x,y: x+y, filtered) / size
    print(result_3)

if '__main__' == __name__:
    
    tree = ET.parse(PARENT_PATH.joinpath('datasets').joinpath('posts.xml'))
    root = tree.getroot()
    data_chunks_1 = chunckify(root, 50)
    data_chunks_2 = chunckify(root, 50)
    data_chunks_3 = chunckify(root, 50)
    
    map_reduce_1(data_chunks_1)
    map_reduce_2(data_chunks_2)
    map_reduce_3(data_chunks_3)
    
    