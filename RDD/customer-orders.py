from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    custId = int(fields[0])
    amount = float(fields[2])
    return (custId, amount)

input = sc.textFile("./../assets/customer-orders.csv")
rdd = input.map(parseLine)
totalAmountSpent = rdd.reduceByKey(lambda x, y: x + y)
totalAmountSpentSortedOnAmount = totalAmountSpent.map(lambda x: (x[1], x[0])).sortByKey()
results = totalAmountSpentSortedOnAmount.collect()
for result in results:
    print(result[1], result[0]);
