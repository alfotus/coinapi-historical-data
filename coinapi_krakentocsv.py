import requests
import csv
import json
import pandas as pd 
import os
import sys
import multiprocessing as mp

global hdr
hdr = {"X-CoinAPI-Key":"B9DB5091-3FDB-4555-81B1-005175BE4DEB"}

def main():
	response=requests.get("https://rest.coinapi.io/v1/symbols?filter_symbol_id=KRAKEN_SPOT",headers=hdr)
	symbollist=[]
	for x in response.json():
		symbollist.append(x["symbol_id"])
	for symbol in symbollist:
		response2 = requests.get("https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1DAY&time_start=2018-12-31T00:00:00&time_end=2019-06-01T00:00:00&limit=10000".format(symbol),headers=hdr)
		print("{} retrieved successfully.".format(symbol))
		data=pd.read_json(json.dumps(response2.json()))
		codedirectory=os.path.dirname(sys.argv[0])
		splittedsymbol=symbol.split("_")[0]
		folderpath=os.path.abspath(codedirectory)+"/"+splittedsymbol+"2/"
		if not os.path.exists(folderpath):
			os.mkdir(folderpath)
		fullpath=folderpath+"{}_{}.csv".format(symbol.split("_")[-2],symbol.split("_")[-1])
		data.to_csv(fullpath)

if __name__=="__main__":
	main()