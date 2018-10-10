
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as funcs


sc = SparkContext(appName="PrevRowDiffApp")
sqlc = SQLContext(sc)
sc.setLogLevel("ERROR")

rdd = sc.textFile("/home/gpurama/Downloads/mock.csv").map(lambda line: line.split(","))
schema = StructType([StructField("customer_id", StringType(), True),
    StructField("fname", StringType(), True),
    StructField("lname", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date", StringType(), True)])


df = sqlc.createDataFrame(rdd, schema)
#print(df.show())

df = df.withColumn("date", df["date"].cast("timestamp"))

#df.select("customer_id").groupBy("date").count().orderBy("customer_id")

df.show()
#print(df.printSchema())

my_window = Window.partitionBy().orderBy("customer_id")

df = df.withColumn("prev_value", F.lag(df.date).over(my_window))

df = df.withColumn("prev_value", df["prev_value"].cast("timestamp"))

df = df.withColumn('daysBetween',funcs.datediff(df.date, df.prev_value))

df.show()
df1=df.filter("daysBetween >=-6 AND daysBetween <=7")
df1.show()
df1.groupBy("customer_id").count().orderBy("customer_id").show()




