import requests
import csv
import json
import pandas as pd 
import sys

parameters = {"X-CoinAPI-Key":"8CFD1A47-9381-4B96-BB8E-8E1421DCF137"}

response = requests.get("https://rest.coinapi.io/v1/ohlcv/BINANCE_SPOT_ETH_BTC/history?period_id=5MIN&time_start=2019-04-26T00:00:00&limit=10000",headers=parameters)

print(response.status_code)

#print(response.content)

data = response.json()

data=pd.read_json(json.dumps(data))

print(type(data))

print(data.columns)

print(data.describe())

print(data.info())

print(data.loc[:,"time_period_start"].sort_values())