import requests
import csv
import json
import pandas as pd 
import os
import sys
import multiprocessing as mp

global headers
headers = {"X-CoinAPI-Key":"AF540657-FCE1-4BD8-BD06-A79EA536A08D"}

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
	volitality_string="COINBASE_SPOT_BTC_EUR,COINBASE_SPOT_BTC_GBP,COINBASE_SPOT_BTC_USD,COINBASE_SPOT_BTC_USDC,COINBASE_SPOT_ETH_BTC,COINBASE_SPOT_ETH_EUR,COINBASE_SPOT_ETH_GBP,COINBASE_SPOT_ETH_USD,COINBASE_SPOT_ETH_USDC,COINBASE_SPOT_XRP_BTC,COINBASE_SPOT_XRP_EUR,COINBASE_SPOT_XRP_USD,COINBASE_SPOT_BCH_BTC,COINBASE_SPOT_BCH_EUR,COINBASE_SPOT_BCH_GBP,COINBASE_SPOT_BCH_USD,COINBASE_SPOT_LTC_BTC,COINBASE_SPOT_LTC_EUR,COINBASE_SPOT_LTC_GBP,COINBASE_SPOT_LTC_USD,COINBASE_SPOT_EOS_BTC,COINBASE_SPOT_EOS_EUR,COINBASE_SPOT_EOS_USD,COINBASE_SPOT_XLM_BTC,COINBASE_SPOT_XLM_EUR,COINBASE_SPOT_XLM_USD"
	url="https://rest.coinapi.io/v1/symbols?filter_symbol_id={}".format(volitality_string)
	response=requests.get(url,headers=headers)
	symbollist=[]
	for x in response.json():
		symbollist.append(x["symbol_id"])
	pool=mp.Pool(mp.cpu_count())
	result=pool.map(getdata,symbollist)		

if __name__=="__main__":
	main()