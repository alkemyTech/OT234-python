from typing import Counter
from functools import reduce
from lib.chunckify import chunckify
from lib.add_timer import get_duration
from lib.OT234_136_functions import get_dates, reducir_contadores, mapper_1
from lib.OT234_136_functions import get_answer_visit, get_post_type_1, get_ratio, mapper_2
import xml.etree.ElementTree as ET
from pathlib  import Path
import copy

PARENT_PATH = Path(__file__).parent.absolute()

@get_duration
def map_reduce_1(data_chunks):
    data= data_chunks
    mapped = map(mapper_1, data)
    counted = map(Counter, mapped)
    reduced = reduce(reducir_contadores, counted).most_common(10)
    print(reduced)

@get_duration
def map_reduce_2(data_chunks):
    data= data_chunks
    mapped = list(map(mapper_2, data_chunks))
    print(mapped)

@get_duration
def map_reduce_3(data_chunks):
    data= data_chunks

if '__main__' == __name__:
    tree = ET.parse(PARENT_PATH.joinpath('datasets').joinpath('posts.xml'))
    root = tree.getroot()
    data_chunks_1 = chunckify(root, 50)
    data_chunks_2 = chunckify(root, 50)
    data_chunks_3 = chunckify(root, 50)
    
    map_reduce_1(data_chunks_1)
    map_reduce_2(data_chunks_2)
    map_reduce_3(data_chunks_3)