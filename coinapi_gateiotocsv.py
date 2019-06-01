import requests
import csv
import json
import pandas as pd 
import os
import sys
import multiprocessing as mp

global hdr
hdr = {"X-CoinAPI-Key":"AF52C2E5-245A-4B43-A253-F56F3193AB3E"}

def getdata(listx):
		response = requests.get("https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1HRS&time_start=2018-06-01T00:00:00&time_end=2019-06-01T00:00:00&limit=10000".format(listx),headers=hdr)
		print("{} retrieved successfully.".format(listx))
		data=pd.read_json(json.dumps(response.json()))
		codedirectory=os.path.dirname(sys.argv[0])
		splittedsymbol=listx.split("_")[0]
		folderpath=os.path.abspath(codedirectory)+"/"+splittedsymbol+"/"
		if not os.path.exists(folderpath):
			os.mkdir(folderpath)
		fullpath=folderpath+"{}_{}.csv".format(listx.split("_")[-2],listx.split("_")[-1])
		data.to_csv(fullpath)

def main():
	response=requests.get("https://rest.coinapi.io/v1/symbols?filter_symbol_id=GATEIO_SPOT",headers=hdr)
	symbollist=[]
	for x in response.json():
		symbollist.append(x["symbol_id"])
	pool=mp.Pool(mp.cpu_count())
	result=pool.map(getdata,symbollist)		

if __name__=="__main__":
	main()