from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import date
from datetime import datetime
from datetime import timedelta
from geopy.distance import vincenty

def first(new, new1): 
    if new == 0:
        new1.next()
    import csv
    reader = csv.reader(new1)
    for row in reader:
        if (row[3][:10] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave'):
            startTime = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S+%f")
            yield startTime, row[0]
            
def second(new, new1):
    if new == 0:
        new1.next()
    import csv
    reader = csv.reader(new1)
    for row in reader:
        if (row[4] != 'NULL') & (row[5] != 'NULL'):
            if (vincenty((40.73901691,-74.00263761), (float(row[4]), float(row[5]))).miles) <= 0.25:
                drop = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")
                time = drop + timedelta(seconds = 600)
                yield drop,time

sc = SparkContext()                
spark = HiveContext(sc)

taxi = sc.textFile('/tmp/yellow.csv.gz').cache()
citibike = sc.textFile('/tmp/citibike.csv').cache()

one = citibike.mapPartitionsWithIndex(first)
two = taxi.mapPartitionsWithIndex(second)

one_data = one.toDF(['start', 'ride'])
two_data = two.toDF(['dropoff', 'buffer'])

unique = two_data.join(one_data).filter((two_data.dropoff < one_data.start) & (two_data.buffer > one_data.start))

print len(unique.toPandas()['ride'].unique())