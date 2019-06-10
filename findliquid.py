import requests

hdr={"X-CoinAPI-Key":"4DF77F9B-6DBA-4E69-8A13-4126161F48B6"}

response=requests.get("https://rest.coinapi.io/v1/exchanges",headers=hdr)

for x in response.json():
	if "liquid" in x["website"]:
		print(x["exchange_id"])
		print(x["website"])