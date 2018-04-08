from pyspark import SparkConf, SparkContext
#from pyspark.sql import SQLContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName('spar_sql_test')
sc = SparkContext(conf = conf)

#sqlContext = SQLContext(sc);
hc = HiveContext(sc);

# Parallelize a list and convert each line to a Row
# Row(id=1, name="a", age=28)
# datas -> Spark RDD source, type = str
datas = ['1 a 28', '2 b 39', '3 c 30']
source = sc.parallelize(datas)
splits = source.map(lambda line: line.split(" "))
rows = splits.map(lambda words: Row(id = words[0], name = words[1], age = words[2]))

# Infer the schema, and register the Schema as a table
people = hc.inferSchema(rows)
people.printSchema()

# SQL can be run over SchemaRDD that have been registered as a table
people.registerTempTable("people")
results = hc.sql('select * from people where age > 28 and age < 30')
results.printSchema()

# The results of SQL queries are SchemaRDD, so register it as a table
results.registerTempTable("people2")
results2 = hc.sql('select name from people2')
results2.printSchema()

# The SchemaRDD support all the normal RDD operations
results3 = results2.map(lambda row:row.name.upper()).collect()

for result in results3:
	print 'name:', result

sc.stop() 

