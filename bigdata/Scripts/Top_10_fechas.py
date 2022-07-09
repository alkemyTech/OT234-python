import xml.etree.ElementTree as ET
from typing import Counter
from functools import reduce
from datetime import datetime


def chunckify(iterable,len_of_chunk):
    for i in range(0,len(iterable), len_of_chunk):
        yield iterable[i:i + len_of_chunk]
        
def obtener_fechas(data):
    creation_date = datetime.strptime(data.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f').date().strftime("%d/%m/%Y")
    post_id = data.attrib['Id']
    return creation_date

def contar_fechas(data):
    return Counter(data)

def reducir_contadores(data1, data2):
    data1.update(data2)
    return data1

def mapper(data):
    fechas = list(map(obtener_fechas, data))
    return fechas

tree = ET.parse('datasets/posts.xml')
root = tree.getroot()
data_chunks = chunckify(root, 50)
mapped = list(map(mapper, data_chunks))
mapped = list(map(Counter, mapped))
reduced = reduce(reducir_contadores, mapped)
result = reduced.most_common(10)
print(result)