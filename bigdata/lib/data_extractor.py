import xml.etree.ElementTree as ET
from functools import reduce  
import datetime  
from pathlib import Path
path = str((Path(__file__).parent.absolute()).parent)

def getActivityTime(row):
    try:
        accepted = row[1].attrib['AnswerCount']
        return [row[1].attrib['Id'],row[1].attrib['CreationDate'],row[1].attrib['LastActivityDate']]
    except:
        pass   
    
def cleanDate(lista):
    return [lista[0],lista[1][0:10].split("-"),lista[2][0:10].split("-")]

def dateToDays(lista):
    """ 
    Using the timestamp() function to convert datetime into epoch time.
    86400 is the amount of seconds on a day
    """  
    return [(datetime.datetime(int(lista[2][0]),int(lista[2][1]),int(lista[2][2])).timestamp() -
            datetime.datetime(int(lista[1][0]),int(lista[1][1]),int(lista[1][2])).timestamp()) / 86400,lista[0]]

def getScoreCount(row):
    try:
        accepted = row[1].attrib['AnswerCount']
        return [row[1].attrib['Score'],row[1].attrib['AnswerCount']]
    except:
        pass   

def scoreReducer (dict_1,dict_2):
    for key in dict_2:   
        if key in dict_1.keys():
            dict_1[key] = dict_1[key] + dict_2[key]
    return dict_1        

def scoreCountPipeline():
    lista = map(getScoreCount,ET.iterparse(
        path + "/datasets/posts.xml")
        )
    lista = filter(None,list(lista))
    score_amount_answers = list(lista)
    score_amount_answers = sorted(score_amount_answers,reverse=True)
    return score_amount_answers[:30]

def topActivityTimePipeline():
    preguntas_tiempo = map(getActivityTime,ET.iterparse(
        path + "/datasets/posts.xml")
        )
    preguntas_tiempo = filter(None,preguntas_tiempo)
    preguntas_tiempo = map(cleanDate,preguntas_tiempo)
    preguntas_tiempo = map(dateToDays,preguntas_tiempo)
    top10_activity_time = sorted(list(preguntas_tiempo),reverse=True)[:10]
    return top10_activity_time

def __main__():
    print("Top 10 (Activity Time (Days) | ID")
    print(topActivityTimePipeline())
    
    print("(Score | Amount Answers) ")
    print(scoreCountPipeline())
    

__main__()
