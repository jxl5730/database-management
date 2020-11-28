#!/usr/bin/env python
# coding: utf-8

# In[13]:


import mysql.connector as mysql
import json
import sys
import csv
import requests
import re 
import unicodedata
import pandas as pd 
import sys
import numpy as np


tables = []
tables_FK = []
tables_PK = []
tables_PK_combined = []

def connect():
    connection = mysql.connect(
        user=credentials.get('username'),
        password=credentials.get('password'),
        database=db,
        host='127.0.0.1'
    )
    return connection
    
def execute(connection, sql):
    cursor = connection.cursor()
    # Execute the query
    cursor.execute(sql)

##GET TABLE NAMES    
def find_tables(connection, db):
    cursor = connection.cursor()
    sql = "select * from information_schema.tables t where t.table_schema = '%s' and t.table_type='BASE TABLE'"
    cursor.execute(sql % db)
    
    for row in cursor:
        tables.append(row[2])
    
##GET FOREIGN KEY RELATIONS
def find_FK(connection, db):
    cursor = connection.cursor()
    sql = "select TABLE_NAME,COLUMN_NAME,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME from information_Schema.key_column_usage where REFERENCED_TABLE_SCHEMA='%s'"
    cursor.execute(sql % db)

    # Current Table, Referencing Col, Referenced Table, Referenced Col
    for row in cursor:
        tables_FK.append([row[0], row[1], row[2], row[3]])
        tables_FK.append([row[2], row[3], row[0], row[1]])
    #print(tables_FK)

##GET PRIMARY KEYS
def find_PK(connection, db):
	cursor = connection.cursor()
	sql = "select TABLE_NAME, SEQ_IN_INDEX, COLUMN_NAME from information_Schema.statistics where TABLE_SCHEMA='%s' and INDEX_NAME='PRIMARY'"
	cursor.execute(sql % db)

	for row in cursor:
		tables_PK.append([row[0], row[1], row[2]]) #tables_name, seq_in_index_of_pk, pk_col_name

def addFk(url):
	for i in range(len(tables_FK)):
		data = {"FKey":tables_FK[i][1], "RefCol":tables_FK[i][3]}
		response = requests.put(url+tables_FK[i][0]+'/FKRel/'+tables_FK[i][2]+'.json', json.dumps(data))
		if response.status_code == 200:
			print("Successfully Added FK to Firebase")
		else:
			print("FK Upload Failure")

##ADD TABLES
def export_table(connection, table_name, db):  
    cursor = connection.cursor()
    columns = []
    old_cols = []
    sql = "select * from information_schema.columns c where c.table_schema='%s' and c.table_name='%s'"
    cursor.execute(sql % (db,table_name))
    
    for row in cursor:
    	old_cols.append([row[4]-1, row[3]])
    old_cols.sort(key=lambda x:x[0])
    for i in old_cols:
    	columns.append(i[1])

    temp_PK = []
    for t_PK in tables_PK: # list of [tables_name, seq_in_index_of_pk, pk_col_name]
    	if t_PK[0] == table_name:
    		temp_PK.append(t_PK[2])

    sql = "select * from %s"
    cursor.execute(sql % table_name)
    data = cursor.fetchall()
        
    
    columns[0] = '# ' + columns[0]
    with open(table_name +'.csv', 'w', newline='',encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer = csv.writer(file, quoting=csv.QUOTE_ALL, quotechar="'")
        for row in data:
            writer.writerow(row)
    
#End of export.py functions. Start of load.py functions

def convertToJson(data):
	data = data.to_json(orient='records')
	return data

##CREATE INVERTED INDEX
def createIndex(data, filteredColumns, indexData, csvFilePath):
    for ind, row in data.iterrows():
        for i in filteredColumns:
            for word in row[i].split(' '):
                word = word.lower()
                word=re.sub('[^a-zA-Z0-9 \n\.]', '', word)
                if word!='':
                    if word not in indexData:
                        indexData[word] = []
                    indexData[word].append({"Table":str(csvFilePath.split(".")[0]), "Column":str(data.columns[i]), "PKey": str(data.columns[0]), "PKeyVal":str(row[0])})

##UPLOAD TO FIREBASE CCONSOLE
def uploadToFirebase(url, data, csvFilePath):

	response = requests.put(url+csvFilePath.split(".")[0]+'.json', data)
	if response.status_code == 200:
		print("Successfully Uploaded to Firebase")
	else:
		print("Upload Failure")

##HANDLE ANY FIREBASE EXCEPTIONS
def cleanItUp(data):
	for _ in data.columns:
		if data[_].dtype == np.int64 or data[_].dtype == np.float64:
			data[_] = data[_].astype(str)

	data.columns = data.columns.map(lambda x: re.sub(r'\W+', '', x))
	filteredColumns = []
	i = 0
	for _ in data.columns:
		if data[_].dtype == np.object:
			data[_] = data[_].astype(str)
			filteredColumns.append(i)
			data[_] = data[_].map(lambda x: re.sub(r'[-[\]/.(\)]', ' ', x))
			data[_] = data[_].map(lambda x: re.sub(r'(& )', ' ', x))
			data[_] = data[_].map(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', errors='ignore').decode('utf-8'))
		i += 1

	return data, filteredColumns	



# end of load.py functions

if __name__ == "__main__": 
	#connect to mysql db
	credentials = {'username':'dsci551', 'password':'dsci551'}
	db = 'music'

	firebase_db = 'music'

	connection = connect()


	#Exporting tables from mysql to csv files
	find_tables(connection, db)
	find_PK(connection, db)
	find_FK(connection, db)
	for table in tables:
	    export_table(connection, table, db)
	    #print("Exported db table to " + table + ".csv")
	        
	# Close the connection
	connection.close()

	#Start of load.py main
	indexData = {}
	url = "https://ds551-f4ff5.firebaseio.com/"+firebase_db+"/"

	#writing the csv files into firebase
	for table in tables:
		csvFilePath = table+".csv"
		try:
			data = pd.read_csv(csvFilePath, encoding='utf-8', quotechar="'", skipinitialspace = True)			
		except:
			data = pd.read_csv(csvFilePath, encoding='latin-1', quotechar="'", skipinitialspace = True)				
		data, filteredColumns = cleanItUp(data)
		json_data = convertToJson(data)
		uploadToFirebase(url, json_data, csvFilePath)
		createIndex(data, filteredColumns, indexData, csvFilePath)

	addFk(url)	
	indexData = json.dumps(indexData)
	response = requests.put(url+'index.json', indexData)
	if response.status_code == 200:
		print("Successfully Uploaded INDEX to Firebase")
	else:
		print("INDEX Upload Failure")
		print(response.text)
    
            


# In[9]:


n_items =list(indexData.keys())
n_items


# In[ ]:




