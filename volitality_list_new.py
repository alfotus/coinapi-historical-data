import os
import pandas as pd
from datetime import datetime
import math

top_ten_data={}
pairing_list=[]
pairing_volume_list=[]
pairing_base_list=[]

exchanges_list=["BINANCE_VOLITALITY","KRAKEN_VOLITALITY","BITSTAMP_VOLITALITY","BITFINEX_VOLITALITY","BITFLYER_VOLITALITY","GEMINI_VOLITALITY","BITTREX_VOLITALITY","ITBIT_VOLITALITY","POLONIEX_VOLITALITY","QUOINE_VOLITALITY","UPBIT_VOLITALITY","GATEIO_VOLITALITY","COINBASE_VOLITALITY"]
#exchanges_list=["GEMINI"]

count=0

for files in os.listdir(os.getcwd()):
	folder_path=os.path.join(os.getcwd(),files)
	folder_name=folder_path.split("/")[-1]
	if os.path.isdir(folder_path) and folder_name in exchanges_list:
		count+=1
		for csv in os.listdir(files):
			pairing=folder_path.split("/")[-1]+"_"+csv
			csv_string=os.path.join(folder_path,csv)
			data=pd.read_csv(csv_string)
			if "volume_traded" in data.columns:
				
				#if pairing.split("_")[-1] not in pairing_base_list:
					#pairing_base_list.append(pairing.split("_")[-1])
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

#for base in pairing_base_list:
#	base_pool=top_ten_pairing["pairing_name"].str.split("_").str[-1].str.startswith(base)
#	if top_ten_pairing[base_pool].shape[0] > 10:
#		print(top_ten_pairing[base_pool].head(10).reset_index(drop=True))

pd.set_option("max_rows",10)
volitality_exchange_list=[]
variance_list=[]
daily_volitality_list=[]
annualized_volitality_list=[]
final_dict={}
lack_data_list=[]
t_volume_list=[]

for exchange in exchanges_list:
	exchange_pool=top_ten_pairing["pairing_name"].str.startswith(exchange)
	sorted_exchange=top_ten_pairing[exchange_pool]
	#btc_pool=sorted_exchange["pairing_name"].str.split("_").str[-1].str.startswith("BTC")
	#sorted_btc=sorted_exchange[btc_pool]
	#if sorted_btc.shape[0] > 1:
	#	print(sorted_btc.head())
	currency=sorted_exchange["pairing_name"].str.split("_",n=2).str[-1]
	print(currency.head())
	for files in os.listdir(os.getcwd()):
		folder_path=os.path.join(os.getcwd(),files)
		folder_name=folder_path.split("/")[-1]
		if os.path.isdir(folder_path) and folder_name == exchange:
			for csv in os.listdir(files):
				for pairing in currency:
					if exchange+"_"+csv == exchange+"_"+pairing:
						csv_string=os.path.join(folder_path,csv)
						data=pd.read_csv(csv_string)
						data["time_period_start"]=pd.to_datetime(data["time_period_start"],format="%Y-%m-%dT%H:%M:%S").tz_localize(None)
						data["time_period_end"]=pd.to_datetime(data["time_period_start"],format="%Y-%m-%dT%H:%M:%S")
						query=pd.Period("2018-12-31/2019-5-31")
						selected_data=data[query.start_time<=data["time_period_start"]]
						filtered_selected_data=selected_data[selected_data["time_period_start"]<=pd.Timestamp(2019,6,1)]
						if filtered_selected_data.empty == False and filtered_selected_data["time_period_start"].iloc[0] == pd.Timestamp(2018,12,31,0,0) and filtered_selected_data["time_period_start"].iloc[-1] == pd.Timestamp(2019,5,31,0):
							print("\n\nfrom "+exchange+"_"+csv)
							test_data=filtered_selected_data.loc[:,["price_close","volume_traded"]]
							test_data.index=filtered_selected_data["time_period_start"]
							test_data.index=pd.to_datetime(test_data.index)
							grouped=test_data.groupby([test_data.index.year,test_data.index.month,test_data.index.day]).sum()
							df_grouped=grouped
							df_grouped["(P(i)-P(avg))"]=df_grouped["price_close"]-(df_grouped["price_close"].mean())
							df_grouped["(P(i)-P(avg))2"]=df_grouped["(P(i)-P(avg))"]*df_grouped["(P(i)-P(avg))"]
							print(df_grouped)
							variance=df_grouped["(P(i)-P(avg))2"].mean()
							daily_volitality=math.sqrt(variance)
							annualized_volitality=math.sqrt(df_grouped.shape[0])*daily_volitality
							t_traded_volume=df_grouped["volume_traded"].sum()
							print("Variance = "+str(variance))
							print("Daily Volitality = "+str(daily_volitality))
							print("Annualized Volitality = "+str(annualized_volitality))
							print("\n")
							volitality_exchange_list.append(exchange+"_"+csv)
							variance_list.append(variance)
							daily_volitality_list.append(daily_volitality)
							annualized_volitality_list.append(annualized_volitality)
							t_volume_list.append(t_traded_volume)
						else:
							lack_data_list.append(exchange+"_"+csv)
							#print(filtered_selected_data["time_period_start"].iloc[0:2])
							#print(filtered_selected_data["time_period_start"].iloc[-3:-1])
						#if selected_data.loc[:,"time_period_start"].iloc[0] is pd.Timestamp(2018,12,31):
						#	print("from "+exchange+"_"+csv)
						#	print(selected_data)
						#print("from "+exchange+"_"+csv+", this is the open time: "+time_period_start_datetime)

final_dict["exchange_pair_name"]=volitality_exchange_list
final_dict["variance"]=variance_list
final_dict["daily_volitality"]=daily_volitality_list
final_dict["annualized_volitality"]=annualized_volitality_list
final_dict["total_volume_traded"]=t_volume_list

final_data=pd.DataFrame(data=final_dict)

volitality_file_path=os.getcwd()+"/volitality_list.csv"

if not os.path.exists(volitality_file_path):
	#os.mkdir(volitality_folder_path)
	final_data.to_csv(volitality_file_path)


missingdata=pd.Series(lack_data_list)
missingdata_file_path=os.getcwd()+"/missingdata_pair.csv"

if not os.path.exists(missingdata_file_path):
	#os.mkdir(volitality_folder_path)
	missingdata.to_csv(missingdata_file_path)

final_data["base"]=final_data["exchange_pair_name"].str.split("_",n=3).str[-1].str.split(".").str[0]
final_data["quote"]=final_data["exchange_pair_name"].str.split("_",n=3).str[2]
final_data_sorted=final_data.sort_values(by=["quote","base","total_volume_traded"],ascending=[True,True,False])
final_data_sorted=final_data_sorted.loc[:,["exchange_pair_name","quote","base","variance","daily_volitality","annualized_volitality","total_volume_traded"]].reset_index(drop=True)
print(final_data_sorted)

sorted_volitality_file_path=os.getcwd()+"/sorted_volitality_list.csv"

if not os.path.exists(sorted_volitality_file_path):
	#os.mkdir(volitality_folder_path)
	final_data_sorted.to_csv(sorted_volitality_file_path)

#print(top_ten_pairing)

#top_ten_pairing.to_csv(os.getcwd()+"/top_ten_pairing.csv")
