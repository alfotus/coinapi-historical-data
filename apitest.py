import requests
import csv
import json
import pandas as pd 
import os
import sys
import multiprocessing as mp

global parameters
parameters = {"X-CoinAPI-Key":"8CFD1A47-9381-4B96-BB8E-8E1421DCF137"}

def applydata(listx):
		response = requests.get("https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1HRS&time_start=2019-04-26T00:00:00&limit=10000".format(listx),headers=parameters)
		print("{}".format(listx))
		data=pd.read_csv(json.dumps(response.json()))

def main():
	response=requests.get("https://rest.coinapi.io/v1/symbols?filter_symbol_id=BINANCE",headers=parameters)
	binancelist=[]
	for x in response.json():
		binancelist.append(x["symbol_id"])
	pool=mp.Pool(mp.cpu_count())
	result=pool.map(applydata,binancelist)		

if __name__=="__main__":
	main()