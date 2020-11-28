from flask import Flask, request, render_template, redirect, url_for
import requests 
import json 
import sys
import pandas as pd
import time

app = Flask(__name__)

##DEAFULT HOMEPAGE AND EDITED HOME PAGE
@app.route('/', methods = ['POST', 'GET'])
def index():
	if request.method == 'POST':
		#data = [dict()]
		data=['music','steam']
		select = request.form.get('db-select')
		search = request.form.get('search')
		
		data.remove(str(select))
		data.insert(0, str(select))

		#GET THE SELECTED DB
		
		db = str(select).lower()
		#GET THE KEYWORD LIST
		query = str(search).split(" ")


		##ONSET OF SEARCH.PY
		url = "https://ds551-f4ff5.firebaseio.com/"+db+"/"

		results = []
		results_count = []
		final_output = []

		tab_results = []
		tab_results_count = []
		tab_final_output = []


		total_time = 0
		total_len = 0
		for i in range(len(query)):
			keyword = query[i].lower()

			temp_res, tab_temp_res , temp_response_time, temp_response_len = retrieve_tuples(keyword, url)

			total_time += temp_response_time
			total_len += temp_response_len

			for r in tab_temp_res:
				if r[1] not in tab_results:
					tab_results.append(r[1])
					tab_results_count.append([1,[r[0]],r[1], r[2]]) 
				else:
					ind = tab_results.index(r[1])
					tab_results_count[ind][0] += 1
					if r[0] not in tab_results_count[ind][1]:
						tab_results_count[ind][1].append(r[0])


		print("#################################")
		print("Total Search Query time: ", total_time)
		print("Total Search Query length: ", total_len)
		#print("Final Output")
		#results_count = sorted(results_count, key=lambda x:x[0], reverse=True)

		tab_results_count.sort(key=lambda x:(-x[0], len(x[1]), x[1][0]))
		prev = None
		for r in tab_results_count:
			resp = get_fk(r[3], url)
			fk = []
			for t in resp:
				fk.append(resp[t]['FKey'])
			fk = list(set(fk))
			if len(tab_final_output)<1:
				tab_final_output.append([r[3],r[2], fk, False])
				prev = r[3]
			else:
				if r[3]==prev:
					tab_final_output.append([r[3],r[2], fk, True])
				else:
					tab_final_output.append([r[3],r[2], fk, False])
					prev = r[3]

		for r in tab_final_output:
			print(r)
			print()



		return render_template("index.html", data = data, output = tab_final_output, database = db, Sq = str(search), time = '{0:.2f}'.format(total_time), l = total_len, fk = False)
	return render_template("index.html", data=['music','steam'])

#RETRIEVE TUPLES FROM FIREBASE BASED ON INPUT
def retrieve_tuples(word, url):
	start = time.time()
	entries = requests.get(url+'index/'+word+'.json')
	response_time = time.time() - start
	response_len = len(entries.content)
	entries = json.loads(entries.text)
	res_tups = []
	col_res_tups = []
	tab_col_res_tups = []
	if not entries:
		print("entries empty")
		return
	for i in range(len(entries)):
		t= entries[i]['Table']
		col = entries[i]['PKey']
		pk = entries[i]["PKeyVal"]
		start = time.time()
		res = requests.get(url+t+'.json?orderBy=\"'+col+'\"&equalTo=\"'+pk+'\"')
		temp_response_time = time.time() - start
		response_time += temp_response_time
		response_len += len(res.content)
		
		res = json.loads(res.text)
		for key in res:
			temp = [entries[i]['Column'],res[key]]
			if temp not in col_res_tups:
				col_res_tups.append(temp)
				tab_col_res_tups.append([entries[i]['Column'], res[key], t])

	return col_res_tups, tab_col_res_tups, response_time, response_len
	
def get_fk(table, url):
	response = requests.get(url+table+'/FKRel.json')
	response = json.loads(response.text)
	return response


@app.route('/<col_name>/<fkey>/<table>/<db_name>/<sq_name>')
def explore(col_name, fkey, table, db_name, sq_name):
	data = ['music','steam']

	data.remove(db_name)
	data.insert(0, db_name)


	output, total_time, total_len = retrieve_next(col_name, fkey, table, db_name)
	print("FK RETRIEVE: ", total_time)
	print("RETRIEVE LEN: ", total_len)

	return render_template("index.html", data= data, output = output, database = db_name, Sq = sq_name, time = '{0:.2f}'.format(total_time), l = total_len, fk = fkey, col = col_name, tab = table)

## EXPLORATION 
def retrieve_next(col_name, fk, table, db_name):

	###GET THE FOREIGN KEY RELATIONS
	url = "https://ds551-f4ff5.firebaseio.com/"+db_name+"/"

	start = time.time()
	response = requests.get(url+table+'/FKRel.json')
	response_time = time.time() - start
	response_len = len(response.content)
	response = json.loads(response.text)


	output = []
	for table in response:
		if response[table]['FKey'] == col_name:
			start = time.time()
			new_tups = requests.get(url+table+'.json?orderBy=\"'+response[table]['RefCol']+'\"&equalTo=\"'+fk+'\"')
			temp_response_time = time.time() - start
			response_time += temp_response_time
			response_len += len(new_tups.content)
			new_tups = json.loads(new_tups.text)
			resp = get_fk(table, url)
			new_fk = []
			for t in resp:
				new_fk.append(resp[t]['FKey'])
			for i in new_tups:
				print(new_tups[i])
				if len(output)<1:
					output.append([table, new_tups[i], new_fk, False])
					prev = table
				else:
					if table == prev:
						output.append([table, new_tups[i], new_fk, True])
					else:
						output.append([table, new_tups[i], new_fk, False])
						prev = table


	for i in output:
		print(i)
		print()
	return output, response_time, response_len


if __name__ == '__main__':
	app.debug = True
	app.run()
	app.run(debug = True)
