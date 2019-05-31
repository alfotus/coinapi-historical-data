import os
import sys
import shutil
import multiprocessing as mp
from itertools import repeat as re
import dateparser
import pytz
import json
import csv
import datetime
from dateutil.rrule import rrule, DAILY
from binance.client import Client

pathname = os.path.dirname(sys.argv[0])
platform = sys.platform

def os_file_prefix(platform,intended_dir):
	if platform == 'win32':
		return '{}\\'.format(intended_dir)
	else:
		return '{}/'.format(intended_dir)

full_path = os_file_prefix(platform,os.path.abspath(pathname))

def currency_data_user_input():
	quote_currrency = grab_quote_currency()
	base_currency = grab_base_currency()
	pair_list=list()
	pair_list.append("{}{}".format(quote_currrency,base_currency))
	return pair_list

def grab_base_currency():
	base_currency_preference = input('Which base currency would you like to grab data for?( BTC, BNB, ETH, or XRP) ')
	return base_currency_preference.upper()

def grab_quote_currency():
	quote_currency_preference = input('Which quote currency would you like to grab data for?( BTC, or any other ALTS coin) ')
	return quote_currency_preference.upper()

def grab_kline_interval():
	kline_interval = input('What Kline Interal would you prefer? Options: 1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h ')
	valid_kline_intervals =['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h']
	if kline_interval in valid_kline_intervals:
		return kline_interval
	else:
		raise ValueError('INVALID KLINE INTERVAL: {} is an invalid option, please try again'.format(str(kline_interval)))

def grab_date_interval():
	start_date_input = input('What date range would you like to pull data from?\nIn MM/DD/YYYY format,except you can enter now for end date to get most recent.\nStart date: ')
	start_date_month,start_date_day,start_date_year= start_date_input.replace('.','/').split('/')
	start_date = datetime.datetime(int(start_date_year),int(start_date_month),int(start_date_day))
	binance_start_date = datetime.datetime(2017,7,1)
	if start_date < binance_start_date:
		raise ValueError('INVALID START DATE: Binance opened in July of 2017, please try a date later than 07/01/2017')
	end_date_input = input('End date: ')
	if end_date_input.lower() in ['now']:
		end_date = datetime.datetime.now()
	else:
		end_date_month,end_date_day,end_date_year = end_date_input.replace('.','/').split('/')
		end_date = datetime.datetime(int(end_date_year),int(end_date_month),int(end_date_day))
	return start_date,end_date

def create_directories(pair_list,kline_interval,start_date,end_date):
	start_date = start_date.strftime('%Y-%m-%d')
	end_date = end_date.strftime('%Y-%m-%d')
	historical_price_data_directory = '{}historical_price_data'.format(str(full_path))
	try:
		os.makedirs(historical_price_data_directory)
	except OSError:
		pass
	kline_interval_directory = ''.join([os_file_prefix(platform,historical_price_data_directory),'{}_{}_{}'.format(str(start_date),str(end_date),str(kline_interval))])
	try:
		os.makedirs(kline_interval_directory)
	except OSError:
		pass
	for x,p in enumerate(pair_list):
		pair_directory = ''.join([os_file_prefix(platform,kline_interval_directory),'{}'.format(str(p))])
		try:
			os.makedirs(pair_directory)
		except OSError:
			pass
	return kline_interval_directory

def process_dates(start_date,end_date):
	end_date = end_date+datetime.timedelta(days=1)
	dates =[date for date in rrule(DAILY,dtstart=start_date, until=end_date)]
	return dates

def grab_data(pair,start_date,end_date,dates,kline_interval_directory,interval,csv_file_info):
	titles = ('Date','Open','High','Low','Close','Volume')
	partial_path = ''.join([os_file_prefix(platform,kline_interval_directory),os_file_prefix(platform,pair)])
	for x,date in enumerate(dates):
		if date != dates[-1]:
			year = str(date.year)
			numerical_month = str(date.month)
			month_abbreviation_dict = {'1':'Jan','2':'Feb','3':'Mar','4':'Apr','5':'May','6':'Jun','7':'Jul','8':'Aug','9':'Sept','10':'Oct','11':'Nov','12':'Dec'}
			calendar_month = month_abbreviation_dict.get(numerical_month,"")
			klines_date = '{}, {}'.format(calendar_month,year)
			start = '{} {}'.format(date.strftime('%d'),klines_date)
			end = '{} {}'.format(dates[x+1].strftime('%d'),klines_date)
			print ('currency pair: {} start: {} end: {}'.format(pair,start,end))
			klines = get_historical_klines(pair, interval, start, end)
			if klines:
				if int(date.day) in range(1,10):
					csv_day = '0{}'.format(str(date.day))
				else:
					csv_day = str(date.day)
				if int(date.month) in range(1,9):
					csv_month ='{}-0{}-'.format(year,numerical_month)
				else:
					csv_month = '{}-{}-'.format(year,numerical_month)
				results_csv = '{}{}{}_{}.csv'.format(str(partial_path),str(csv_month),str(csv_day),str(interval))
				with open(results_csv, 'a') as f:
						writer = csv.writer(f)
						writer.writerow(titles)
				for x,k in enumerate(klines):
					if k !=klines[-1]:
						open_timestamp,open_,high,low,close_,volume,close_timestamp,quote_asset_volume,num_trades,taker_buy_base_asset_volume,taker_buy_quote_asset_volume,ignore = k
						open_time = datetime.datetime.utcfromtimestamp(float(open_timestamp)/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
						fields = (open_time,open_,high,low,close_,volume)
						with open(results_csv, 'a') as f:
							writer = csv.writer(f)
							writer.writerow(fields)
	file_retrevial_info = pair,partial_path,start_date.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'),interval
	csv_file_info.append(file_retrevial_info)

def get_historical_klines(symbol, interval, start_str, end_str=None):
	client = Client(None, None)
	output_data = []
	limit = 1000
	timeframe = interval_to_milliseconds(interval)
	start_ts = date_to_milliseconds(start_str)
	end_ts = None
	if end_str:
		end_ts = date_to_milliseconds(end_str)
	idx = 0
	symbol_existed = False
	while True:
		try:
			temp_data = client.get_klines(symbol=symbol,interval=interval,limit=limit,startTime=start_ts,endTime=end_ts)
			if not symbol_existed and len(temp_data):
				symbol_existed = True
			if symbol_existed:
				output_data += temp_data
				start_ts = temp_data[len(temp_data) - 1][0] + timeframe
			else:
				start_ts += timeframe
			idx += 1
		except Exception as e:
			print (str(e))
			idx+=1
		if len(temp_data) < limit:
			break
	return output_data

def interval_to_milliseconds(interval):
	ms = None
	seconds_per_unit = {'m': 60,'h': 60 * 60,'d': 24 * 60 * 60,'w': 7 * 24 * 60 * 60}
	unit = interval[-1]
	if unit in seconds_per_unit:
		try:
			ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
		except ValueError:
			pass
	return ms

def date_to_milliseconds(date_str):
	epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
	d = dateparser.parse(date_str)
	if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
		d = d.replace(tzinfo=pytz.utc)
	return int((d - epoch).total_seconds() * 1000.0)

def concatenate_csvs(csv_file_info):
	for x,file_info in enumerate(csv_file_info):
		pair,partial_path,start_date,end_date,interval = file_info
		individual_csvs_directory = '{}individual_csvs'.format(str(partial_path))
		try:
			os.makedirs(individual_csvs_directory)
		except OSError:
			pass
		csv_files = [f for f in os.listdir('{}'.format(partial_path)) if os.path.isfile(os.path.join('{}'.format(partial_path), f))]
		csv_files = sorted(csv_files)
		concat_csv = '{}_{}_{}_{}.csv'.format(str(pair),str(start_date),str(end_date),str(interval))
		if concat_csv in csv_files:
			old_concat_csvs_path = '{}old_concatenated_csvs'.format(str(partial_path))
			try:
				os.makedirs(old_concat_csvs_path)
			except OSError:
				pass
			shutil.move('{}{}'.format(str(partial_path),str(concat_csv)),'{}/{}'.format(str(old_concat_csvs_path),str(concat_csv)))
			csv_files = [f for f in os.listdir('{}'.format(str(partial_path))) if os.path.isfile(os.path.join('{}'.format(str(partial_path)), f))]
			csv_files = sorted(csv_files)
		if csv_files:
			for x,csv_file in enumerate(csv_files):
				outpath = '{}{}'.format(str(partial_path),str(concat_csv))
				fout=open(outpath,'a')
				full_file_path = '{}{}'.format(str(partial_path),str(csv_file))
				writer = csv.writer(fout,lineterminator='\n')
				with open(full_file_path) as f:
					if x != 0:
						f.__next__()
					for line in f:
						if len(line)>1:
							timestamp,open_,high,low,close_,volume = line.split(',')
							writer.writerow([timestamp,open_,high,low,close_,volume.strip()])
				f.close()
				fout.close()
				shutil.move(full_file_path,''.join([os_file_prefix(platform,individual_csvs_directory) ,'{}'.format(csv_file)]))


def main():
	csv_file_info = mp.Manager().list()
	pair_list = currency_data_user_input()
	interval = grab_kline_interval()
	start_date,end_date = grab_date_interval()
	kline_interval_directory = create_directories(pair_list,interval,start_date,end_date)
	dates = process_dates(start_date,end_date)
	pair = [currency_pair for i,currency_pair in enumerate(pair_list)]
	lock = mp.Lock()
	pool = mp.Pool(processes=mp.cpu_count(),initargs=(lock,))
	data = pool.starmap(grab_data,zip(pair,re(start_date),re(end_date),re(dates),re(kline_interval_directory),re(interval),re(csv_file_info)))
	pool.close()
	pool.join()
	concatenate_csvs(list(set(csv_file_info)))

if __name__ == '__main__':
	main()