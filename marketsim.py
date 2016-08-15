'''
Create a market simulation tool that takes a command line like this:
python marketsim.py 1000000 orders.csv values.csv
'''

import pandas as pd
import pandas.io.parsers as pd_parser
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import sys

def main():
    readCSV()



def readCSV():

	order_sheet = pd_parser.read_csv(sys.argv[2], header=None)
	print order_sheet
	ls_symbols = list(set(order_sheet[3].values))

	# Need to sort the trades DF by increasing date
	order_sheet = order_sheet.sort([0,1,2])

	# Getting the start and end dates from the .csv file
	dt_start = dt.datetime( order_sheet.head(1)[0], order_sheet.head(1)[1], order_sheet.head(1)[2])
	dt_end = dt.datetime( order_sheet.tail(1)[0], order_sheet.tail(1)[1], order_sheet.tail(1)[2] + 1 )
	startCash = int(sys.argv[1])
	print startCash
	dataobj = da.DataAccess('Yahoo', cachestalltime=0)
	ls_keys = ['close', 'actual_close']
	ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

	ldf_data = dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
	d_data = dict(zip(ls_keys, ldf_data))
	# Missing Values
	for s_key in ls_keys:
    		d_data[s_key] = d_data[s_key].fillna(method = 'ffill')
    		d_data[s_key] = d_data[s_key].fillna(method = 'bfill')
    		d_data[s_key] = d_data[s_key].fillna(1.0)

	# Adding CASH to our symbols and creating our trades table
	ls_symbols.append("_CASH")
	trades_data = pd.DataFrame(index=list(ldt_timestamps), columns=list(ls_symbols))

	curr_cash = startCash
	trades_data["_CASH"][ldt_timestamps[0]] = startCash
	print("Llegamos")
	curr_stocks = dict()
	for sym in ls_symbols:
		curr_stocks[sym] = 0
		trades_data[sym][ldt_timestamps[0]] = 0

	for row in order_sheet.iterrows():
		row_data = row[1]
		curr_date = dt.datetime(row_data[0], row_data[1], row_data[2], 16 )
		sym = row_data[3]
		stock_value = d_data['close'][sym][curr_date]
		stock_amount = row_data[5]

		if row_data[4] == "Buy":
			to_substract=stock_value *stock_amount
			print to_substract
			curr_cash = curr_cash - to_substract
			trades_data["_CASH"][curr_date] = curr_cash
			curr_stocks[sym] = curr_stocks[sym] + stock_amount
			trades_data[sym][curr_date] = curr_stocks[sym]
		else:
			curr_cash = curr_cash + (stock_value * stock_amount)
			trades_data["_CASH"][curr_date] = curr_cash
			curr_stocks[sym] = curr_stocks[sym] - stock_amount
			trades_data[sym][curr_date] = curr_stocks[sym]


	trades_data = trades_data.fillna(method = "pad")

	value_data = pd.DataFrame(index=list(ldt_timestamps), columns=list("V"))
	value_data = value_data.fillna(0)

	for day in ldt_timestamps:
		value = 0

		for sym in ls_symbols:
			if sym == "_CASH":
				value = value + trades_data[sym][day]
			else:
				value = value + trades_data[sym][day] * d_data['close'][sym][day]

		value_data["V"][day] = value

	file_out = open( sys.argv[3], "w" )
	for row in value_data.iterrows():
		file_out.writelines(str(row[0].strftime('%Y,%m,%d')) + "," + str(row[1]["V"].round()) + "\n" )

	file_out.close()


if __name__ == "__main__":
    main()