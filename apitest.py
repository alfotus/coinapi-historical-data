import requests
import csv
import json
import pandas as pd 
import sys

parameters = {"X-CoinAPI-Key":"8CFD1A47-9381-4B96-BB8E-8E1421DCF137"}

response = requests.get("https://rest.coinapi.io/v1/exchanges",headers=parameters)

print(response.status_code)

#print(response.content)

data = response.json()

data=pd.read_json(json.dumps(data))

print(type(data))

print(data.columns)