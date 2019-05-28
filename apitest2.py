import requests
import csv
import json
import pandas as pd 
import sys
import os

parameters = {"X-CoinAPI-Key":"8CFD1A47-9381-4B96-BB8E-8E1421DCF137"}

#response=requests.get("https://rest.coinapi.io/v1/symbols?filter_symbol_id=BINANCE",headers=parameters)
response = requests.get("https://rest.coinapi.io/v1/ohlcv/BINANCE_SPOT_ETH_BTC/history?period_id=1HRS&time_start=2018-05-28T00:00:00&limit=10000",headers=parameters)

data=pd.read_json(json.dumps(response.json()))

print(type(data))

codedirectory=os.path.dirname(sys.argv[0])

splittedsymbol="BINANCE_SPOT_ETH_BTC".split("_")[0]

folderpath=os.path.abspath(codedirectory)+"/"+splittedsymbol+"/"

#print(folderpath)
if not os.path.exists(folderpath):
	os.mkdir(folderpath)

fullpath=os.path.abspath(codedirectory)+"/"+splittedsymbol+"/"+"BINANCE_SPOT_ETH_BTC"+".csv"
#print(fullpath)
data.to_csv(fullpath)