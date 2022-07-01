# University Database Processing - Data Normalization
######## Data Format sought:
#  university: str minúsculas, sin espacios extras, ni guiones
#  career: str minúsculas, sin espacios extras, ni guiones
#  inscription_date: str %Y-%m-%d format
#  first_name: str minúscula y sin espacios, ni guiones
#  last_name: str minúscula y sin espacios, ni guiones
#  gender: str choice(male, female)
#  age: int
#  postal_code: str
#  location: str minúscula sin espacios extras, ni guiones
#  email: str minúsculas, sin espacios extras, ni guiones

import requests
import datetime as dt
from dateutil import parser
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import re


'''
raw_path : absolute path to raw  (ex: '../airflow/files/')
datasets_path : absolute path to processed  (ex: '../airflow/datasets/')
Univ: University (ex: 'UPal')
'''

def Process_UPal(Univ, raw_path, datasets_path):
    # Path to .csv files
    csv_path = raw_path + Univ + '_dump.csv'

    ### Raw .csv Data gathering
    DRaw = pd.read_csv(csv_path,encoding='ISO-8859-1') 

    ## Data Table Creation - Normalized Data
    Cols = ['university','career','inscription_date','first_name','last_name','gender','age','postal_code','location','email']
    DNor = pd.DataFrame(columns = Cols)  

    for rws in range(0, len(DRaw)):
        university = 'universidad de palermo'   # University
        career = re.sub('_',' ',DRaw.careers[rws])
        inscription_date = DRaw.fecha_de_inscripcion[rws] 

        full_name = DRaw.names[rws].split('_')  # Names
        if len(full_name) == 2 :     #          
            first_name = full_name[0]
            last_name = full_name[1]

        if DRaw.sexo[rws] == 'm': gender = 'male'   # Gender
        else: gender = 'female'

        Hoy = dt.datetime.now()                  # Age
        BDate = parser.parse(DRaw.birth_dates[rws])  
        if relativedelta(Hoy,BDate).years < 12:
            BDate = BDate - relativedelta(years=100)
            age = relativedelta(Hoy,BDate).years
        else: 
            age = relativedelta(Hoy,BDate).years     

        Addr_String = DRaw.direcciones[rws].splitlines()[1]    # Address string splitting
        try: 
            postal_code = re.findall(r"\d+", Addr_String.split(',')[1]).pop(0)
        except: 
            postal_code = re.findall(r"\d+", Addr_String).pop(0)
        location = re.sub('_',' ', Addr_String.split(',')[0])
    
        email = DRaw.correos_electronicos[rws]

        addRow = pd.DataFrame(np.array([[university,career,inscription_date,first_name,last_name,gender,age,postal_code,location,email]]),columns = Cols) # Row
        DNor = pd.concat([DNor,addRow], axis = 0, join='inner', ignore_index= True) # Row addition


    ## Save dataset to .txt file
    # Path to datasets .txt files
    txt_path = datasets_path + Univ + '_dataset.txt'

    DNor.to_csv(txt_path, header=True, index=None, sep='\t', mode='w')