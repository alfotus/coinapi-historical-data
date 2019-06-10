import requests
import csv
import json
import pandas as pd 
import os
import sys
import multiprocessing as mp


def getdata(listx):
	url2="https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1DAY&time_start=2018-12-31T00:00:00&time_end=2019-06-01T00:00:00&limit=100000".format(listx)
	headers2 = {"X-CoinAPI-Key":"CB9258DF-6897-4C34-A303-165A762CCDBA"}
	response2 = requests.get(url2,headers=headers2)
	print(response2.status_code)
	print("{} retrieved successfully.".format(listx))
	data=json.loads(response2.text)
	data=pd.DataFrame(data)
	codedirectory=os.path.dirname(sys.argv[0])
	splittedsymbol=listx.split("_")[0]
	folderpath=os.path.abspath(codedirectory)+"/"+splittedsymbol+"2/"
	if not os.path.exists(folderpath):
		os.mkdir(folderpath)
	fullpath=folderpath+"{}_{}.csv".format(listx.split("_")[-2],listx.split("_")[-1])
	data.to_csv(fullpath)

def main():
	url="https://rest.coinapi.io/v1/symbols?filter_symbol_id=BINANCE_SPOT"
	headers = {"X-CoinAPI-Key":"FDE47235-9F75-4C22-99E7-748213CD30F0"}
	response=requests.get(url,headers=headers)
	symbollist=[]
	for x in response.json():
		symbollist.append(x["symbol_id"])
	pool=mp.Pool(mp.cpu_count())
	result=pool.map(getdata,symbollist)		

if __name__=="__main__":
	main()