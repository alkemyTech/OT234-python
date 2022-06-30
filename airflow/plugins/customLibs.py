import pandas as pd
import numpy as np
import csv
import boto3

import os
 
# get current directory
exec_path = os.getcwd()

cp_csv = exec_path + "/plugins/codigos_postales.csv"

def sqlFileToQuery(path):
    query = ''
    with open(path) as file:
        for line in file:
            query += line
    return query

def cpDict(path):
    with open(path) as file:
        reader = csv.reader(file)
        cpDict = {rows[0]:rows[1] for rows in reader}
        
    return cpDict

def BaseProcessing(df):
    
    df.university = df.university.str.replace('_',' ')
    df.career = df.career.str.replace('_',' ')
    df.inscription_date = df.inscription_date.str.replace('/','-')
    df.first_name = df.first_name.str.replace(' ','').str.replace('-','').str.upper()
    df.last_name = df.last_name.str.replace(' ','').str.replace('-','').str.upper()
    df.gender = df.gender.str.replace('m','male').replace('f','female')
    print(df.age.head())
    df.age = df.age.apply(lambda x: 0 if x.split(' ')[0] == "0:00:00" else int(x.split(' ')[0])//365).astype(int)
    df.age = df.age.apply(lambda x: x if x > 0 else x + 100)
    if df.postal_code.dtypes != 'int':
        df.postal_code = df.postal_code.apply(lambda x: int(x.split(' ')[-1]))
    
    df.email = df.email.str.replace(' ','').str.replace('-','').str.lower()
    return df

def postalCode(df):
    cp_dict = cpDict(cp_csv)
    df.location = df.postal_code.apply(lambda x : cp_dict[str(x)]).str.lower()
    return df

def exportToTxt(df,path):
    print(path)
    np.savetxt(path, df.values, fmt='%s',delimiter=',')

def cleaningPipeline(**kwargs):

    input_path = kwargs['input_path']
    postal_fix = kwargs['postal_fix']
    output_path = kwargs['output_path']

    df = pd.read_csv(input_path,sep='|')

    df = BaseProcessing(df)
    if postal_fix == True:
        df = postalCode(df)
    
    exportToTxt(df,output_path)

def loadAws(**kwargs):
    print('hola')
    file_path = kwargs['file_path']
    bucket = kwargs['bucket']
    file_name = file_path.split('/')[-1]
    print(file_name)
    data = open(file_path, 'rb')


    s3 = boto3.resource('s3')
    s3.Bucket(bucket).put_object(Key=file_name, Body=data)