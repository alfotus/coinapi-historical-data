import requests
import csv
import json
import pandas as pd 
import sys

parameters = {"X-CoinAPI-Key":"8CFD1A47-9381-4B96-BB8E-8E1421DCF137"}

response = requests.get("https://rest.coinapi.io/v1/exchanges",params=parameters)

print(response.status_code)

#print(response.content)

data = response.json()

print(data)

print(type(data))

pd.read_json(json.dumps(data)).to_csv("apitest.csv")