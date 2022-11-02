from pyspark import SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
#from pyspark.sql import SQLContext


# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext(appName="WordCountFiltered")
spark = SparkSession(sc)
#sql_context = SQLContext(spark_session.sparkContext)
logFile = "file:/home/xjin12/cs425_mp4/test/network_medium.txt"
#logFile = "file:/Users/alanjin/GoogleDrivex/CurDesktop/CS425/MPs/cs425_mp4/test/network_medium.txt"
staticDatabase= "file:/home/xjin12/cs425_mp4/static_database02.txt"
#staticDatabase= "file:/Users/alanjin/GoogleDrivex/CurDesktop/CS425/MPs/cs425_mp4/static_database02.txt"
# Create a DStream that will connect to hostname:port, like localhost:9999
lines = sc.textFile(logFile).cache()
linesDatabase = sc.textFile(staticDatabase).cache()

# Split each line into words
words = lines.map(lambda line: tuple(line.strip().replace("\t"," ").split(" ")))
wordsDatabase = linesDatabase.map(lambda line: tuple(line.strip().replace("\t"," ").split(" ")))



schema = StructType([StructField('fr', StringType()),StructField('to', StringType())])
df1 = spark.createDataFrame(words, ['fr', 'to'])
dfDatabase = spark.createDataFrame(wordsDatabase,['fr', 'to'])
df1.createTempView('df1')
dfDatabase.createTempView('dfDatabase')

df = dfDatabase.join(df1, (df1.fr == dfDatabase.to))

df.show()

# f = open("spark_test_out","w")
# #wordCounts.saveAsTextFile("spark_test_out")
# for pair in wordCounts.collect():
#         key = pair[0]
#         value = pair[1]
#         f.write("{} {}\n".format(key, value))
# f.close()
