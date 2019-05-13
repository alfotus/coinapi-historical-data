import os
import sys

pathname=os.path.dirname(sys.argv[0])
print(pathname)

platform = sys.platform
print(platform)

full_path="{}\\".format(str(os.path.abspath(pathname)))
print(full_path)

def grab_base_currency():
	base_currency_preference = input('Which base currency would you like to grab data for?( BTC, ETH, BNB, USDT, PAX, TUSD, XRP, or USDC) ')
	return base_currency_preference.upper()

def grab_quote_currency():
	quote_currency_preference = input('Which quote currency would you like to grab data for? ')
	return quote_currency_preference.upper()

def grab_kline_interval():
	kline_interval = input('What Kline Interal would you prefer? Options: 1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h ')
	if kline_interval in ['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h']:
		return kline_interval
	else:
		print ('{} is an invalid option, please try again'.format(str(kline_interval)))
		kline_interval_2 = input('What Kline Interal would you prefer? Options: 1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h ')
		if kline_interval_2 in ['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h']:
			return kline_interval_2

def grab_date_interval():
	print ('What date range would you like to pull data from?\nIn MM/DD/YYYY format,except you can enter now for end date to get most recent.')
	start_date_input = input('Start date: ')
	start_date_replaced = str(start_date_input).replace('.','/')
	start_date_split = start_date_replaced.split('/')
	start_date_month = int(start_date_split[0])
	start_date_day = int(start_date_split[1])
	start_date_year = int(start_date_split[2])
	start_date = datetime.date(start_date_year,start_date_month,start_date_day)
	binance_start_date = datetime.date(2017,7,1)
	if start_date < binance_start_date:
		start_date_input = input('Binance opened in July of 2017, please try a date later than 07/01/2017: ')
		start_date_replaced = str(start_date_input).replace('.','/')
		start_date_split = start_date_replaced.split('/')
		start_date_month = int(start_date_split[0])
		start_date_day = int(start_date_split[1])
		start_date_year = int(start_date_split[2])
		start_date = datetime.date(start_date_year,start_date_month,start_date_day)
		if start_date < binance_start_date:
			print ('Error, please restart and be sure to enter dates in MM/DD/YYYY format')
			quit()
	end_date_input = input('End date: ')
	if end_date_input.lower() in ['now']:
		end_date = datetime.datetime.now()
		end_date_string = str(end_date)
		end_date_string_split = end_date_string.split('-')
		end_date_month = end_date_string_split[1]
		end_date_day = end_date_string_split[2][:2]
		end_date_year = end_date_string_split[0]
		end_date_delta = datetime.date(int(end_date_year),int(end_date_month),int(end_date_day))
		delta = end_date_delta-start_date
		days_between = delta.days
	else:
		end_date_replaced = end_date_input.replace('.','/')
		end_date_split = end_date_replaced.split('/')
		end_date_month = int(end_date_split[0])
		end_date_day = int(end_date_split[1])
		end_date_year = int(end_date_split[2])
		end_date = datetime.date(end_date_year,end_date_month,end_date_day)
		delta = end_date-start_date
		days_between = delta.days
	return start_date,end_date,start_date_year,start_date_month,start_date_day,end_date_year,end_date_month,end_date_day,days_between

if __name__="__main__":
	pair_list=list()
	base_currency_to_grab=grab_base_currency()
	quote_currency_to_grab = grab_quote_currency()
	symbol = '{}{}'.format(str(quote_currency_to_grab),str(base_currency_to_grab))
	pair_list.append(symbol)

	interval=grab_kline_interval()

	start_date,end_date,start_date_year,start_date_month,start_date_day,end_date_year,end_date_month,end_date_day,days_between = grab_date_interval()

	kline_interval_directory = create_directories(pair_list,interval,start_date,end_date)