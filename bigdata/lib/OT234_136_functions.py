from typing import Counter
from datetime import datetime

def get_duration(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        func(*args, **kwargs)
        end = datetime.now()
        time = end - start
        print('Took ', time.total_seconds(), ' s')
    return wrapper

# Enunciado 1

def get_dates(data):
    creation_date = datetime.strptime(data.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f').date().strftime("%d/%m/%Y")
    post_id = data.attrib['Id']
    return creation_date

def reducir_contadores(row_1, row_2):
    row_1.update(row_2)
    return row_1

def mapper_1(data):
    dates = map(get_dates, data)
    return dates

# Enunciado 2

def get_answer_visit(data):
    try: 
        answers = data.attrib['AnswerCount']
    except KeyError:
        answers = 0
    visits = data.attrib['ViewCount']
    post_id = data.attrib['Id']
    return post_id, answers, visits 

def get_post_type_1(data):
    if data.attrib['PostTypeId'] == "1":
        return True
    return False
    
def get_ratio(data):
    return data[0], int(data[1])/int(data[2])

def mapper_2(data):
    filtered = list(filter(get_post_type_1, data))
    resp_visit = list(map(get_answer_visit, filtered))
    ratio = list(map(get_ratio, resp_visit))
    return ratio

# Enunciado 3

def get_creation_date(data):
    date = datetime.strptime(data.attrib['CreationDate'], '%Y-%m-%dT%H:%M:%S.%f')
    if int(data.attrib['Score']) > 100:
        return None
    elif data.attrib['PostTypeId'] ==  '1':
        return {data.attrib['Id']:[date, None]}
    elif data.attrib['PostTypeId'] ==  '2':
        return {data.attrib['ParentId']: [None, date]}

def mapper_3(data):
    mapped = list(map(get_creation_date, data))
    mapped = list(filter(None, mapped))
    return mapped

def reducer(data1, data2):
    data1.extend(data2)
    return data1

def posts_reducer(data1, data2):
    data_1 = data1.copy()
    data_2 = data2.copy()
    key_2 = list(data_2.keys())[0]    
    if data_2[key_2][1] == None or data_1.get(key_2) == None:
        data_1.update(data_2)
        return data_1
    elif data_1[key_2][1] == None:
        data_1[key_2][1] = data_2[key_2][1]
        return data_1
    elif data_1[key_2][1] < data_2[key_2][1]:
        data_1[key_2][1] = data_2[key_2][1]
    return data_1

def get_answer_time(data):
    try: 
        return (data[1]-data[0]).total_seconds()
    except TypeError:
        return None