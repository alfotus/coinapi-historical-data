import pandas as pd
import os


exchanges_list=["BINANCE_VOLITALITY","KRAKEN_VOLITALITY","BITSTAMP_VOLITALITY","BITFINEX_VOLITALITY","BITFLYER_VOLITALITY","GEMINI_VOLITALITY","BITTREX_VOLITALITY","ITBIT_VOLITALITY","POLONIEX_VOLITALITY","QUOINE_VOLITALITY","UPBIT_VOLITALITY","GATEIO_VOLITALITY","COINBASE_VOLITALITY"]

def find_repeated_pair(exch_list):
	single_pair_list=[]
	seen={}
	unique_pair=[]
	repeated_pair_list=[]
	for files in os.listdir(os.getcwd()):
		folder_path=os.path.join(os.getcwd(),files)
		folder_name=folder_path.split("/")[-1]
		if os.path.isdir(folder_path) and folder_name in exchanges_list:
			for csv in os.listdir(files):
				if os.stat(os.path.join(folder_path,csv)).st_size>10:
					if csv.split("_")[-1].split(".")[0]=="BTC" or csv.split("_")[-1].split(".")[0]=="USD":
							single_pair_list.append(csv)
	for csv in single_pair_list:
		if csv not in seen:
			seen[csv]=1
		else:
			if seen[csv]==1:
				unique_pair.append(csv)
				seen[csv]+=1
	for files in os.listdir(os.getcwd()):
		folder_path=os.path.join(os.getcwd(),files)
		folder_name=folder_path.split("/")[-1]
		if os.path.isdir(folder_path) and folder_name in exchanges_list:
			for csv in os.listdir(files):
				if os.stat(os.path.join(folder_path,csv)).st_size>10:
					if csv.split("_")[-1].split(".")[0]=="BTC" or csv.split("_")[-1].split(".")[0]=="USD":
						if csv in unique_pair:
							repeated_pair_list.append(folder_name+"_"+csv)


	return repeated_pair_list

def read_data(pair_list):
	df_list=[]
	for files in os.listdir(os.getcwd()):
		folder_path=os.path.join(os.getcwd(),files)
		folder_name=folder_path.split("/")[-1]
		if os.path.isdir(folder_path) and folder_name in exchanges_list:
			for csv in os.listdir(files):
				if os.stat(os.path.join(folder_path,csv)).st_size>10:
					if folder_name+"_"+csv in pair_list:
						csv_string=os.path.join(folder_path,csv)
						data=pd.read_csv(csv_string)
						data.index=data["time_period_start"]
						data.index=pd.to_datetime(data.index).tz_localize(None)
						data=data.drop(["Unnamed: 0"],axis=1)
						data.name=folder_name+"_"+csv
						if data.index[0]==pd.Timestamp(2018,12,31) and data.index[-1]==pd.Timestamp(2019,5,31):
							df_list.append(data)
						else:
							missing_day_list.append(data.name)
	return df_list


if __name__=="__main__":
	missing_day_list=[]
	comparable_pair=find_repeated_pair(exchanges_list)
	comparable_pair_df_list=read_data(comparable_pair)
	pd.set_option("max_rows",10)
	print(len(comparable_pair_df_list))
	print(missing_day_list)


