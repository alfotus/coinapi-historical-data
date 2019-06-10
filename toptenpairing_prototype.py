import os
import pandas as pd

top_ten_data={}
pairing_list=[]
pairing_volume_list=[]
pairing_base_list=[]

exchanges_list=["BINANCE","KRAKEN","BITSTAMP","BITFINEX","BITFLYER","GEMINI","BITTREX","ITBIT","POLONIEX","LIQUID","UPBIT","GATEIO","COINBASE"]

count=0

for files in os.listdir(os.getcwd()):
	folder_path=os.path.join(os.getcwd(),files)
	folder_name=folder_path.split("\\")[-1]
	if os.path.isdir(folder_path) and folder_name in exchanges_list:
		count+=1
		for csv in os.listdir(files):
			pairing=folder_path.split("\\")[-1]+"_"+csv
			csv_string=os.path.join(folder_path,csv)
			data=pd.read_csv(csv_string)
			if "volume_traded" in data.columns:
				if pairing.split("_")[-1] not in pairing_base_list:
					pairing_base_list.append(pairing.split("_")[-1])
					#print(data.loc[:,"volume_traded"].sum())
					#total_volume.append([pairing,data.loc[:,"volume_traded"].sum()])
				print(pairing+"[{}/13]".format(count))
				pairing_list.append(pairing)
				pairing_volume_list.append(data.loc[:,"volume_traded"].sum())

top_ten_data["pairing_name"]=pairing_list
top_ten_data["total_volume"]=pairing_volume_list
#top_ten_data["Percentage "]
volume_data=pd.DataFrame(data=top_ten_data)

top_ten_pairing=volume_data.sort_values(by=["total_volume"],ascending=False).reset_index(drop=True)

for base in pairing_base_list:
	base_pool=top_ten_pairing["pairing_name"].str.split("_").str[-1].str.startswith(base)
	if top_ten_pairing[base_pool].shape[0] > 10:
		print(top_ten_pairing[base_pool].head(10).reset_index(drop=True))




#print(top_ten_pairing)

#top_ten_pairing.to_csv(os.getcwd()+"/top_ten_pairing.csv")
