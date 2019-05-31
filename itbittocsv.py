import requests
import csv
import json
import pandas as pd 
import os
import sys
import multiprocessing as mp

global parameters
parameters = {"X-CoinAPI-Key":"EDA2641E-918D-4FF8-BE92-B7B3F3C8C5F4"}

def getdata(listx):
		response = requests.get("https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1HRS&time_start=2019-04-26T00:00:00&limit=10000".format(listx),headers=parameters)
		print("{}".format(listx))
		data=pd.read_csv(json.dumps(response.json()))
		fullpath=folderpath+"{}.csv".format(listx)
		data.to_csv(fullpath)

def main():
	response=requests.get("https://rest.coinapi.io/v1/symbols?filter_symbol_id=ITBIT",headers=parameters)
	binancelist=[]
	codedirectory=os.path.dirname(sys.argv[0])
	folderpath=os.path.abspath(codedirectory)+"\\ITBIT\\"
	if not os.path.exists(folderpath):
		os.mkdir(folderpath)
	for x in response.json():
		binancelist.append(x["symbol_id"])
	pool=mp.Pool(mp.cpu_count())
	result=pool.map(getdata,binancelist)		

if __name__=="__main__":
	main()