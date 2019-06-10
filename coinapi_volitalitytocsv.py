import requests
import csv
import json
import pandas as pd 
import os
import sys
import multiprocessing as mp

global headers
headers = {"X-CoinAPI-Key":"E0A30DEC-3049-4126-983A-507BAEFBDB72"}

def getdata(listx):
	url2="https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1DAY&time_start=2018-12-31T00:00:00&time_end=2019-06-01T00:00:00&limit=100000".format(listx)
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
	volitality_string="POLONIEX_SPOT_BTC_USDC,POLONIEX_SPOT_BTC_USDT,POLONIEX_SPOT_ETH_BTC,POLONIEX_SPOT_ETH_USDC,POLONIEX_SPOT_ETH_USDT,POLONIEX_SPOT_XRP_BTC,POLONIEX_SPOT_XRP_USDC,POLONIEX_SPOT_XRP_USDT,POLONIEX_SPOT_BCH_BTC,POLONIEX_SPOT_BCH_ETH,POLONIEX_SPOT_BCH_USDC,POLONIEX_SPOT_BCH_USDT,POLONIEX_SPOT_LTC_BTC,POLONIEX_SPOT_LTC_USDC,POLONIEX_SPOT_LTC_USDT,POLONIEX_SPOT_LTC_XMR,POLONIEX_SPOT_EOS_BTC,POLONIEX_SPOT_EOS_ETH,POLONIEX_SPOT_EOS_USDT,POLONIEX_SPOT_BCHSV_BTC,POLONIEX_SPOT_BCHSV_USDC,UPBIT_SPOT_BTC_KRW,UPBIT_SPOT_BTC_USDT,UPBIT_SPOT_ETH_BTC,UPBIT_SPOT_ETH_KRW,UPBIT_SPOT_ETH_USDT,UPBIT_SPOT_XRP_BTC,UPBIT_SPOT_XRP_ETH,UPBIT_SPOT_XRP_KRW,UPBIT_SPOT_XRP_USDT,UPBIT_SPOT_BCH_BTC,UPBIT_SPOT_BCH_ETH,UPBIT_SPOT_BCH_KRW,UPBIT_SPOT_BCH_USDT,UPBIT_SPOT_LTC_BTC,UPBIT_SPOT_LTC_ETH,UPBIT_SPOT_LTC_KRW,UPBIT_SPOT_LTC_USDT,UPBIT_SPOT_EOS_KRW,UPBIT_SPOT_BCHSV_BTC,UPBIT_SPOT_BCHSV_KRW,UPBIT_SPOT_XLM_BTC,UPBIT_SPOT_XLM_ETH,UPBIT_SPOT_XLM_KRW,UPBIT_SPOT_TRX_BTC,UPBIT_SPOT_TRX_ETH,UPBIT_SPOT_TRX_KRW,UPBIT_SPOT_TRX_USDT,UPBIT_SPOT_ADA_BTC,UPBIT_SPOT_ADA_ETH,UPBIT_SPOT_ADA_KRW,UPBIT_SPOT_ADA_USDT"
	url="https://rest.coinapi.io/v1/symbols?filter_symbol_id={}".format(volitality_string)
	response=requests.get(url,headers=headers)
	symbollist=[]
	for x in response.json():
		symbollist.append(x["symbol_id"])
	pool=mp.Pool(mp.cpu_count())
	result=pool.map(getdata,symbollist)		

if __name__=="__main__":
	main()