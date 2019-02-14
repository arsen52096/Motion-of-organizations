__author__ = 'arsen52096'

import pyodbc
import pandas as pd
import numpy as np
import datetime
import shutil
import os
import sys

from сonstants_check import *


def operation_with_os(path, oper):
    """
    in folder 'Database' can be a lot of trash files,
    this function take all files and return required mdb

    """
    os.chdir(path)
    directory = os.listdir(path)
    
    try:
        for i in range(len(directory)):
        
            if 'ОУ_СГУК' in directory[i]:
                OPER_PATH_OU = os.path.abspath(directory[i])  
            elif 'CIAC' in directory[i]:
                CIAC = os.path.abspath(directory[i])
            elif 'OperAM' in directory[i]:
                OPER_PATH_NEW = os.path.abspath(directory[i])
        if oper == 'CIAC':
            return CIAC
        else:
            return OPER_PATH_NEW  

    except:
        print('Whats going on in your folder, guy?')


def connect_with_mdb(OPER_PATH):
    """
    connecting to mdb by pyodbc

    """
    try:
        db = pyodbc.connect(
            'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+OPER_PATH
            )
        dbc = db.cursor()
        return dbc
    except:
        print('Error coonect with base')


def execute_RAO_form(dbc, date1):

    base = dbc.execute('SELECT '+list_RAO+' \
FROM SprORG INNER JOIN (FORMP INNER JOIN RAO ON FORMP.ID = RAO.IDF) \
ON SprORG.ID = FORMP.IDP \
WHERE RAO.OpDate > '+date1+'  AND RAO.OpCod >20 AND RAO.OpCod <40')
#RAO.OpDate < '+date1+' AND RAO.OpDate > '+date2+'    
    
    return base

def execute_ROZ_form(dbc, date1):
    
    base = dbc.execute('SELECT '+list_ROZ+' \
FROM SprORG INNER JOIN (FORMP INNER JOIN ROZ ON FORMP.ID = ROZ.IDF) \
ON SprORG.ID = FORMP.IDP \
WHERE ROZ.OpDate > '+date1+' AND ROZ.OpCod >20 AND ROZ.OpCod <40')
    
    return base


def create_dataframe_ROZ(base):
    
    try:
        ROZ = pd.DataFrame(np.array(
            [row for row in base.fetchall()]
            ), columns = COLS_1_5).fillna(0)

        ROZ['Документ, дата'] = ROZ['Документ, дата'].apply(
            lambda x: datetime.datetime.strptime(
                str(x), '%Y-%m-%d %H:%M:%S'
                ).strftime('%d.%m.%Y') if isinstance(
                x, datetime.datetime
                ) else 'Error'
            )


        ROZ['Форма'] = 1.5
        
        return ROZ
    
    except:
        print('Error in create dataframe ROZ')




def create_dataframe_RAO(base):

    RAO = pd.DataFrame(np.array(
        [row for row in base.fetchall()]
    ), columns = COLS_1_6).fillna(0)

    RAO['Документ, дата'] = RAO['Документ, дата'].apply(
            lambda x: datetime.datetime.strptime(
                str(x), '%Y-%m-%d %H:%M:%S'
                ).strftime('%d.%m.%Y') if isinstance(
                x, datetime.datetime
                ) else 'Error'
                )

    RAO['Форма'] = 1.6

    return RAO


def first_normalisations(name, list_main: list, list_contra: list):

    general = set()
    list_of_org = []
    list_of_contra = []

    for i in list_main:
        for j in list_contra:
            if j == i:
                general.add(j)

    general_list = list(general)

    for i in list_main:
        if i not in general_list:
            list_of_org.append(i)

    for i in list_contra:
        if i not in general_list:
            list_of_contra.append(i)


    if name == 'general':
        return general_list
    elif name == 'org':
        return list_of_org
    elif name == 'contra':
        return list_of_contra