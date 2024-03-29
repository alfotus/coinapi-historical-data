import requests
import os
import sys

parameters = {"X-CoinAPI-Key":"B0357E04-98ED-42EE-B1B4-2DA64FC19ED6"}

inputted_symbol=input("\nBINANCE, KRAKEN, BITSTAMP, BITFINEX,\nBITFLYER, GEMINI, BITTREX, ITBIT,\nPOLONIEX, LIQUID, UPBIT, GATEIO\n or MANY OTHERS.....\n\nPlease enter desired exchanges from above: ").upper()

response=requests.get("https://rest.coinapi.io/v1/symbols?filter_symbol_id={}".format(inputted_symbol),headers=parameters)	

count = 0

print("\n",end="")

for x in response.json():
	print(x["symbol_id"])
	count+=1

print("\nTotal pairings:{}\n".format(count))