from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext(appName="WordCount")
logFile = "file:/home/xjin12/cs425_mp4/test/demo.txt"
# Create a DStream that will connect to hostname:port, like localhost:9999
lines = sc.textFile(logFile).cache()
# Split each line into pair
pairs = lines.map(lambda x: (x.strip().split()[0],1))

wordCounts = pairs.reduceByKey(lambda x, y: x + y)

f = open("spark_test_out","w")
#wordCounts.saveAsTextFile("spark_test_out")
for pair in wordCounts.collect():
        key = pair[0]
        value = pair[1]
        f.write("{} {}\n".format(key, value))
f.close()
