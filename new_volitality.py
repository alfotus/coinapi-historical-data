import pandas as pd
import os
import numpy as np
import math
import seaborn as sns
import matplotlib.pyplot as plt

sns.set()

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
	count=0
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
						data=data["2018-12-14":"2019-06-15"]
						if pd.Timestamp(2018,12,16) in data.index and data.index.shape[0]==184:
							data=data.drop(["Unnamed: 0"],axis=1)
							data.name=folder_name+"_"+csv
							if data.index[0]==pd.Timestamp(2018,12,14) and data.index[-1]==pd.Timestamp(2019,6,15):
								df_list.append(data)
							else:
								missing_day_list.append(data.name)
	return df_list


if __name__=="__main__":
	missing_day_list=[]
	namelist=[]
	dailymeanlist=[]
	dailyminlist=[]
	dailymaxlist=[]
	dailysdlist=[]
	dailyskewlist=[]
	dailykurtlist=[]
	dailyobslist=[]
	dailydict={}
	monthlymeanlist=[]
	monthlyminlist=[]
	monthlymaxlist=[]
	monthlysdlist=[]
	monthlyskewlist=[]
	monthlykurtlist=[]
	monthlyobslist=[]
	monthlydict={}
	rvdfdatalist=[]
	comparable_pair=find_repeated_pair(exchanges_list)
	comparable_pair_df_list=read_data(comparable_pair)
	#pd.set_option("max_rows",10)
	for df in comparable_pair_df_list:
		name=df.name
		newseries=df["price_close"].iloc[:-1].dropna()
		newseries.index=df.index[1:]
		df["previous_day_price_close"]=newseries
		df["daily_return_in_percentage"]=100*(np.log(df["price_close"])-np.log(df["previous_day_price_close"]))
		df["daily_squared_return_in_percentage"]=df["daily_return_in_percentage"]*df["daily_return_in_percentage"]
		print(name)
		df=df[1:]
		#print(df)
		namelist.append(name)
		dailymeanlist.append(df["daily_return_in_percentage"].mean())
		dailyminlist.append(df["daily_return_in_percentage"].min())
		dailymaxlist.append(df["daily_return_in_percentage"].max())
		dailysdlist.append(df["daily_return_in_percentage"].std())
		dailyskewlist.append(df["daily_return_in_percentage"].skew())
		dailykurtlist.append(df["daily_return_in_percentage"].kurt())
		dailyobslist.append(df["daily_return_in_percentage"].count())
		rvlist=[]
		temptimelist=[]
		rvdf={}
		for x in [5,4,3,2,1,12]:
			total=0
			count=-1
			temptime=df.index[df.index.day==15]
			if x is not 12:
				temptime=temptime[temptime.month == x+1]
			else:
				temptime=temptime[temptime.month == 1]
			temptimelist.append(temptime)
			pointtime=df.index[df.index.day==16]
			pointtime=pointtime[pointtime.month == x]
			while pointtime<=temptime:
				#print(temptime)
				total+=df.loc[temptime,"daily_squared_return_in_percentage"][0]
				count+=1
				#print(df.loc[temptime,"daily_squared_return_in_percentage"])
				temptime-=pd.Timedelta(days=1)
			rv=math.sqrt(total/count)*math.sqrt(365)
			rvlist.append(rv)
			print("\n")
		rvdf["time"]=temptimelist
		rvdf["rv"]=rvlist
		rvdfdata=pd.DataFrame(data=rvdf)
		rvdfdata.name=name
		rvdfdatalist.append(rvdfdata)
		print(rvlist[::-1])
		rvlist=pd.Series(rvlist[::-1])
		print(rvlist)
		monthlymeanlist.append(rvlist.mean())
		monthlyminlist.append(rvlist.min())
		monthlymaxlist.append(rvlist.max())
		monthlysdlist.append(rvlist.std())
		monthlyskewlist.append(rvlist.skew())
		monthlykurtlist.append(rvlist.kurt())
		monthlyobslist.append(rvlist.count())
		#print(sum)
		print("\n")

	dailydict["name"]=namelist
	dailydict["mean"]=dailymeanlist
	dailydict["min"]=dailyminlist
	dailydict["max"]=dailymaxlist
	dailydict["sd"]=dailysdlist
	dailydict["skew"]=dailyskewlist
	dailydict["kurt"]=dailykurtlist
	dailydict["obs"]=dailyobslist
	dailydf=pd.DataFrame(data=dailydict)

	monthlydict["name"]=namelist
	monthlydict["mean"]=monthlymeanlist
	monthlydict["min"]=monthlyminlist
	monthlydict["max"]=monthlymaxlist
	monthlydict["sd"]=monthlysdlist
	monthlydict["skew"]=monthlyskewlist
	monthlydict["kurt"]=monthlykurtlist
	monthlydict["obs"]=monthlyobslist
	monthlydf=pd.DataFrame(data=monthlydict)

	dailydf["exchange"]=dailydf["name"].str.split("_").str[0]
	dailydf["base"]=dailydf["name"].str.split("_").str[-1].str.split(".").str[0]
	dailydf["quote"]=dailydf["name"].str.split("_").str[-2]
	dailydf["quote_base"]=dailydf["quote"]+"_"+dailydf["base"]
	dailydf=dailydf.loc[:,["exchange","quote_base","quote","base","mean","min","max","sd","skew","kurt","obs"]]
	dailydf=dailydf.sort_values(by=["base","quote","exchange"]).reset_index(drop=True)

	monthlydf["exchange"]=monthlydf["name"].str.split("_").str[0]
	monthlydf["base"]=monthlydf["name"].str.split("_").str[-1].str.split(".").str[0]
	monthlydf["quote"]=monthlydf["name"].str.split("_").str[-2]
	monthlydf["quote_base"]=monthlydf["quote"]+"_"+monthlydf["base"]
	monthlydf=monthlydf.loc[:,["exchange","quote_base","quote","base","mean","min","max","sd","skew","kurt","obs"]]
	monthlydf=monthlydf.sort_values(by=["base","quote","exchange"]).reset_index(drop=True)


	for pair in dailydf["quote_base"].unique():
		legendlist=[]
		for df in comparable_pair_df_list:
			if pair in df.name:
				name=df.name.split("_")[0]
				#+"_"+df.name.split(".")[0].split("_")[2]+"_"+df.name.split(".")[0].split("_")[3]
				legendlist.append(name)
				df=df[1:]
				#print(df)
				p1=sns.kdeplot(df["daily_return_in_percentage"])
		p1.legend(legendlist)
		p1.set_title("Daily returns volatility of {} in various exchange".format(pair))
		p1.set_ylabel("Density")
		p1.set_xlabel("Daily return volatility (Percentage)")
		plt.show()
			
	for pair in monthlydf["quote_base"].unique():
		legendlist=[]
		for df in rvdfdatalist:
			if pair in df.name:
				name=df.name.split("_")[0]
				#+"_"+df.name.split(".")[0].split("_")[2]+"_"+df.name.split(".")[0].split("_")[3]
				legendlist.append(name)
				p1=sns.kdeplot(df["rv"])
		p1.legend(legendlist)
		p1.set_title("Monthly annualised volatility of {} in various exchange".format(pair))
		p1.set_ylabel("Density")
		p1.set_xlabel("Monthly annualised volatility (Percentage)")
		plt.show()


#print(dailydf.count())
#print(monthlydf)

folderpath=os.getcwd()+"/MODIFIED_VOLATILITY/"
if not os.path.exists(folderpath):
	os.mkdir(folderpath)

for df in comparable_pair_df_list:
	name=df.name.split("_")[0]+"_MODIFIED_"+df.name.split(".")[0].split("_")[2]+"_"+df.name.split(".")[0].split("_")[3]
	if not os.path.exists(folderpath+name+".csv"):
		df.to_csv(folderpath+name+".csv")

folderpath2=os.getcwd()+"/MONTHLY_VOLATILITY/"
if not os.path.exists(folderpath2):
	os.mkdir(folderpath2)

for df in rvdfdatalist:
	name=df.name.split("_")[0]+"_MONTHLY_MODIFIED_"+df.name.split(".")[0].split("_")[2]+"_"+df.name.split(".")[0].split("_")[3]
	if not os.path.exists(folderpath2+name+".csv"):
		df.to_csv(folderpath2+name+".csv")

dailydf.to_csv(os.getcwd()+"/daily_returns_volatility.csv")
monthlydf.to_csv(os.getcwd()+"/monthly_annualised_volatility.csv")

			#if df.name.str.contains(pair):
			#	print(df)


#print(monthlylist)


#print(df)
#print(monthlydf)
	#print(comparable_pair_df_list)


		
		#df["change_in_percentage"]=df["price_close"]/df["previous_day_price_close"]-1
		#newseries=df["change_in_percentage"].iloc[:-1].dropna()
		#newseries.index=df.index[1:]
		#df["daily_return_volatility"]=(df["change_in_percentage"]*df["change_in_percentage"]+newseries*newseries)/2
		#df=df[1:]

		#std=df["change_in_percentage"].std()
		#print("\nFrom {}.".format(name))
		#print(df)

		#df["previous_price_close"]=df["price_close"].iloc[-1:]
		#print(df.index[1:])


