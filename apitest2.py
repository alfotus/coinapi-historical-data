import requests
import csv
import json
import pandas as pd 
import sys

parameters = {"X-CoinAPI-Key":"8CFD1A47-9381-4B96-BB8E-8E1421DCF137"}

response=requests.get("https://rest.coinapi.io/v1/symbols?filter_symbol_id=BINANCE",headers=parameters)

data=response.json()
count=0

for x in data:
	print(x["symbol_id"])
	count+=1

print(count)
