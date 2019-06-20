import requests
import csv
import json
import pandas as pd 
import os
import sys
import multiprocessing as mp

global headers
headers = {"X-CoinAPI-Key":"B9A1260F-B23A-4E88-B311-D83C0725E040"}

def getdata(listx):
	url2="https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1DAY&time_start=2018-10-14T00:00:00&time_end=2019-06-16T00:00:00&limit=100000".format(listx)
	response2 = requests.get(url2,headers=headers)
	print(response2.status_code)
	print("{} retrieved successfully.".format(listx))
	data=json.loads(response2.text)
	data=pd.DataFrame(data)
	codedirectory=os.path.dirname(sys.argv[0])
	splittedsymbol=listx.split("_")[0]
	folderpath=os.path.abspath(codedirectory)+"/"+splittedsymbol+"_VOLITALITY/"
	if not os.path.exists(folderpath):
		os.mkdir(folderpath)
	fullpath=folderpath+"{}_{}.csv".format(listx.split("_")[-2],listx.split("_")[-1])
	data.to_csv(fullpath)

def main():
	volitality_string="BITFINEX_SPOT_DAI_ETH,GATEIO_SPOT_PAX_BTC,POLONIEX_SPOT_USDT_USDC"
	url="https://rest.coinapi.io/v1/symbols?filter_symbol_id={}".format(volitality_string)
	response=requests.get(url,headers=headers)
	symbollist=[]
	for x in response.json():
		symbollist.append(x["symbol_id"])
	pool=mp.Pool(mp.cpu_count())
	result=pool.map(getdata,symbollist)		

if __name__=="__main__":
	main()