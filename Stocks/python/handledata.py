import pandas_datareader as web
from operator import add
import pandas as pd
import datetime
import numpy as np
import urllib
import codecs
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
conf = SparkConf().setAppName("handledata").setMaster("yarn")
sc = SparkContext(conf=conf)

def handledata(i):
	try:
		reader = i[1]
		a = reader.loc[reader["signal"]==1]
		count = len(a)
		logreturns = np.log(reader["Close"] / reader["Close"].shift(1))
		vol = np.sqrt(252*logreturns.var())
	except:
		return [i[0],0.00,1.00]
	return  [i[0],count*1.00,vol]	

tmp1 = sc.pickleFile("hdfs:///data_set")
tmp = sc.parallelize(tmp1.collect())
tmp = tmp.map(handledata)
max_count =np.max( tmp.map(lambda x:x[1]).collect()  )
max_vol	  =np.max( tmp.map(lambda x:x[2]).collect()  )
fin_rdd = tmp.map(lambda x:[x[0],x[1]/max_count,x[2]])
fin_rdd = fin_rdd.map(lambda x:[x[0],x[1],1-x[2]/max_vol])
fin_rdd1 = fin_rdd.map(lambda x:[x[0],(x[1]+x[2])/2])

result = sorted(fin_rdd1.collect(), key=lambda x:x[1],reverse=True)

recommend = []
print "recommendable stocks:"
for i in range(5):
	print result[i][0]
	recommend.append(result[i][0])
rdd_show = sc.parallelize(recommend)
rdd_show.saveAsPickleFile("hdfs:///result")
