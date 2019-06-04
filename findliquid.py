import requests

hdr={"X-CoinAPI-Key":"09967F63-E8F2-4088-B2DA-FB699902B2F7"}

response=requests.get("https://rest.coinapi.io/v1/exchanges",headers=hdr)

for x in response.json():
	if "coinbase" in x["website"]:
		print(x["exchange_id"])
		print(x["website"])