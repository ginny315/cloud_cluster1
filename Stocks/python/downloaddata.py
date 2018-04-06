import pandas_datareader as web
from operator import add
import pandas as pd
import datetime
import numpy as np
import urllib
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
conf = SparkConf().setAppName("getdata").setMaster("yarn")
sc = SparkContext(conf=conf)

def ohlc_adj(dat):
    return pd.DataFrame({"Open": dat["Open"] * dat["Adj Close"] / dat["Close"],
                       "High": dat["High"] * dat["Adj Close"] / dat["Close"],
                       "Low": dat["Low"] * dat["Adj Close"] / dat["Close"],
                       "Close": dat["Adj Close"]})

def getdata(symbol):
	start = datetime.datetime(2017,1,1)
	end = datetime.date.today()
	try:
		ori_reader = web.DataReader(symbol, "yahoo", start, end)
		reader=ohlc_adj(ori_reader)
		reader["20d"] = np.round(reader["Close"].rolling(window=20, center=False).mean(), 2)
        	reader["50d"] = np.round(reader["Close"].rolling(window=50, center=False).mean(), 2)
        	reader["20d-50d"] = reader['20d'] - reader['50d']
		reader["signal"] = np.where(reader['20d-50d'] > 0, 1.00, 0.00)
		a = reader.loc[reader["signal"]==1]
		count = len(a)
		logreturns = np.log(reader["Close"] / reader["Close"].shift(1))
		vol = np.sqrt(252*logreturns.var())
		url = "https://finance.yahoo.com/quote/"+symbol+"/profile?p="+symbol
                response = urllib.urlopen(url)
                html = response.read().decode('utf-8')
		a = html.find("Description<")+69
    		b = html.find("</p>",a)
    		result = str(html[a:b])
		reader["description"] = result
		reader["relate data"] = html
	except:
		return [symbol,1]
	return  [symbol,reader] 

stock = ['DIS','INTC','TSLA','AMD','WDC','OSTK','RAD','ROKU','NVDA','PYPL','GM','AAPL','GOOG','PIH','AAON','AEY']
sl=sc.parallelize(stock)
r=sl.map(getdata)
r.saveAsPickleFile("/data_set")
print r.collect()
