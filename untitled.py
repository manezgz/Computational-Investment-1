# QSTK Imports
import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da

# Third Party Imports
import datetime as dt
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
from scipy.optimize import minimize

print "Pandas Version", pd.__version__




def simulate(dt_start,dt_end,ls_symbols,ls_allocations):
	#Prepare data for statistics
    d_data = readData(dt_start, dt_end, ls_symbols)[0];

    #Get numpy ndarray of close prices (numPy)
    na_price = d_data['close'].values;
    na_normalized_price = na_price / na_price[0,:];
    lf_Stats = calcStats(na_normalized_price, ls_allocations);
    print "Sharpe Ratio ",lf_Stats[2]

	
def readData(dt_start,dt_end,ls_symbols):
    dt_timeofday = dt.timedelta(hours=16);

    #Get a list of trading days between the start and end dates (QSTK)
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday);
    c_dataobj = da.DataAccess('Yahoo', cachestalltime=0);

    #Keys to be read from the data
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close'];

    #Read the data and map it to ls_keys via dict() (i.e. Hash Table structure)
    ldf_data = c_dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys);
    d_data = dict(zip(ls_keys, ldf_data));
    return d_data

def calcStats(na_normalized_price, lf_allocations):
    #Calculate cumulative daily portfolio value
    #row-wise multiplication by weights
    na_weighted_price = na_normalized_price * lf_allocations;
    #row-wise sum
    na_portf_value = na_weighted_price.copy().sum(axis=1);

    #Calculate daily returns on portfolio
    na_portf_rets = na_portf_value.copy()
    tsu.returnize0(na_portf_rets);

    #Calculate volatility (stdev) of daily returns of portfolio
    f_portf_volatility = np.std(na_portf_rets); 

    #Calculate average daily returns of portfolio
    f_portf_avgret = np.mean(na_portf_rets);

    #Calculate portfolio sharpe ratio (avg portfolio return / portfolio stdev) * sqrt(252)
    f_portf_sharpe = (f_portf_avgret / f_portf_volatility) * np.sqrt(250);

    #Calculate cumulative daily return
    #...using recursive function
    def cumret(t, lf_returns):
        #base-case
        if t==0:
            return (1 + lf_returns[0]);
        #continuation
        return (cumret(t-1, lf_returns) * (1 + lf_returns[t]));
    f_portf_cumrets = cumret(na_portf_rets.size - 1, na_portf_rets);

    return [f_portf_volatility, f_portf_avgret, f_portf_sharpe, f_portf_cumrets, na_portf_value];    


def main():
        print "Inside Main"
        ls_symbols = ['AAPL', 'GOOG', 'IBM', 'MSFT']
        dt_start = dt.datetime(2011, 1, 1)
        ls_allocations=[0.2,0.3,0.3,0.2]
        simulate(dt_start,dt.datetime(2011, 12, 31),ls_symbols,ls_allocations)

if __name__ == "__main__":
    main()

