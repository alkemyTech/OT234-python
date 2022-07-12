from typing import Counter
from datetime import datetime

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