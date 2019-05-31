import requests
import os
import sys

parameters = {"X-CoinAPI-Key":"EDA2641E-918D-4FF8-BE92-B7B3F3C8C5F4"}
response=requests.get("https://rest.coinapi.io/v1/symbols?filter_symbol_id=ITBIT",headers=parameters)	

count = 0

for x in response.json():
	print(x["symbol_id"])
	count+=1

print(count)

codedirectory=os.path.dirname(sys.argv[0])
folderpath=os.path.abspath(codedirectory)+"\\ITBIT\\"

print(codedirectory)
print(folderpath)