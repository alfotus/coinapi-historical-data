import requests
import csv
import json
import pandas as pd 
import os
import sys
import multiprocessing as mp

global parameters
parameters = {"X-CoinAPI-Key":"D8050D2E-0CAD-467F-80ED-2FE3CC712740"}

def getdata(listx):
		response = requests.get("https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1HRS&time_start=2019-04-26T00:00:00&limit=10000".format(listx),headers=parameters)
		print("{}".format(listx))
		data=pd.read_csv(json.dumps(response.json()))
		codedirectory=os.path.dirname(sys.argv[0])
		splittedsymbol=listx.split("_")[0]
		folderpath=os.path.abspath(codedirectory)+"/"+splittedsymbol+"/"
		if not os.path.exists(folderpath):
			os.mkdir(folderpath)
		fullpath=folderpath+"{}.csv".format(listx)
		data.to_csv(fullpath)

def main():
	response=requests.get("https://rest.coinapi.io/v1/symbols?filter_symbol_id=KRAKEN",headers=parameters)
	binancelist=[]
	for x in response.json():
		binancelist.append(x["symbol_id"])
	pool=mp.Pool(mp.cpu_count())
	result=pool.map(getdata,binancelist)		

if __name__=="__main__":
	main()