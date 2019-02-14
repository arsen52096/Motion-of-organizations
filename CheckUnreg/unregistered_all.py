__author__ = 'arsen52096_/'

import pyodbc
import pandas as pd
import numpy as np
import datetime
import shutil
import os
import sys
from сonstants import *


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


def execute_CIAC_form(base):

    try:
        base = base.execute('SELECT Организация.OKPO FROM Организация')
        return base
    except:
        print('Error in query expression')


def create_dataframe_CIAC(base):
    try:
        CIAC = pd.DataFrame(np.array([row for row in base.fetchall()]), 
            columns = COLS_CIAC).fillna(0)
        return CIAC
    except:
        print('DataFrame dont create')


def searchable_in_list(column1, column2):
    result = set()
    searchable = set(column2)
    
    for number in column1:
        if number not in searchable:
            result.add(number)
    
    return result


def execute_ROZ_form(base):
    
    try:
        base = base.execute('SELECT '+list_ROZ+' \
FROM SprORG INNER JOIN (FORMP INNER JOIN ROZ ON FORMP.ID = ROZ.IDF) ON SprORG.ID = FORMP.IDP \
WHERE FORMP.ID > 0 AND ROZ.OpCod >20 AND ROZ.OpCod <40')
        return base
    except:
        print('Error in query expression for ROZ')


def execute_RAO_form(base):

    try:
        base = base.execute('SELECT '+list_RAO+' \
FROM SprORG INNER JOIN (FORMP INNER JOIN RAO ON FORMP.ID = RAO.IDF) \
ON SprORG.ID = FORMP.IDP \
WHERE FORMP.ID > 0 AND RAO.OpCod >20 AND RAO.OpCod <40')
        return base
    except:
        print('Error in query expression for RAO')


def create_dataframe_ROZ(base):
    
    try:
        ROZ = pd.DataFrame(np.array(
            [row for row in base.fetchall()]
            ), columns = COLS_1_5).fillna(0)
        
        ROZ['Активность, Бк'] = ROZ['Активность, Бк'].apply(
            lambda x: '%.3g' % float(x)
            )
        
        ROZ['Операция, дата'] = ROZ['Операция, дата'].apply(
            lambda x: datetime.datetime.strptime(
                str(x), '%Y-%m-%d %H:%M:%S'
                ).strftime('%d.%m.%Y') if isinstance(
                x, datetime.datetime
                ) else 'Error'
            )
        
        #ROZ['Дата выпуска'] = ROZ['Дата выпуска'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y'))
        
        ROZ['Документ, дата'] = ROZ['Документ, дата'].apply(
            lambda x: datetime.datetime.strptime(
                str(x), '%Y-%m-%d %H:%M:%S'
                ).strftime('%d.%m.%Y') if isinstance(
                x, datetime.datetime
                ) else 'Error'
            )
        
        #ROZ['Форма'] = 1.5
        return ROZ
    except:
        print('Error in create dataframe ROZ')



def create_dataframe_RAO(base):
    
    try:
        RAO = pd.DataFrame(np.array(
            [row for row in base.fetchall()]
            ), columns = COLS_1_6).fillna(0)
        
        RAO['Суммарная активность, альфа-излучение, Бк'] = RAO['Суммарная активность, альфа-излучение, Бк'].apply(
            lambda x: float('%.3g' % x)
            )
        
        #RAO['Тип РАО_1'] = RAO['Тип РАО_1'].apply(lambda x: ''.join(list(x)[8:10]))
        
        RAO['Суммарная активность, бета-излучение, Бк'] = RAO['Суммарная активность, бета-излучение, Бк'].apply(
            lambda x: float('%.3g' % x)
            )


        RAO['Операция, дата'] = RAO['Операция, дата'].apply(
            lambda x: datetime.datetime.strptime(
                str(x), '%Y-%m-%d %H:%M:%S'
                ).strftime('%d.%m.%Y') if isinstance(
                x, datetime.datetime
                ) else 'Error'
            )
        #RAO['Дата измерения активности'] = RAO['Дата измерения активности'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%Y'))
        
        RAO['Документ, дата'] = RAO['Документ, дата'].apply(
            lambda x: datetime.datetime.strptime(
                str(x), '%Y-%m-%d %H:%M:%S'
                ).strftime('%d.%m.%Y') if isinstance(
                x, datetime.datetime
                ) else 'Error'
                )
        
        #RAO['Форма'] = 1.6
    except:
        print('Error in create dataframe RAO')
        
    return RAO



if __name__ == '__main__':
    print(sys.executable)
    path_of_bases = 'o:\\РАО\\Арсен\\DataBase'
    

    ROZ = create_dataframe_ROZ(execute_ROZ_form(connect_with_mdb(operation_with_os(path_of_bases, 'her'))))
    RAO = create_dataframe_RAO(execute_RAO_form(connect_with_mdb(operation_with_os(path_of_bases, 'her'))))
    CIAC = create_dataframe_CIAC(execute_CIAC_form(connect_with_mdb(operation_with_os(path_of_bases, 'CIAC'))))



    c = np.array(RAO['ОКПО, поставщика или получателя']).tolist()
    d = np.array(CIAC['ОКПО']).tolist()
    
    ROZ_column = np.array(ROZ['ОКПО, поставщика или получателя']).tolist()
    

    list_OKPO = searchable_in_list(c,d)
    list_OKPO_ROZ = searchable_in_list(ROZ_column,d)
    
    
    ROZ = ROZ.loc[ROZ['ОКПО, поставщика или получателя'].isin(list_OKPO_ROZ)]
    RAO = RAO.loc[RAO['ОКПО, поставщика или получателя'].isin(list_OKPO)]

    
    os.chdir('o:\\РАО\\Арсен\\CheckUnreg')
    writer = pd.ExcelWriter('Unregistered_new.xlsx', engine='xlsxwriter')
    ROZ.to_excel(writer, sheet_name = 'ROZ')
    RAO.to_excel(writer, sheet_name = 'RAO')
    writer.save()