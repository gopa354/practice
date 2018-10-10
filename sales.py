from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
#import pyspark.sql.functions.unix_timestamp

sc=SparkContext() 
sqlContext = SQLContext(sc)

lines = sc.textFile("/home/gpurama/Spark_task/spark-test/sales.txt")
parts = lines.map(lambda l: l.split("|"))

#ts = unix_timestamp("dts","MM/dd/yyyy HH:mm:ss").cast("timestamp")
people = parts.map(lambda p: Row(transaction_id=int(p[0]),customer_id=int(p[1]),product_id=int(p[2]),timestamp=p[3],total_amount=int(p[4]),total_quantity=int(p[5])))

schemaPeople = sqlContext.createDataFrame(people)
print(schemaPeople.show())

mode = "overwrite"
url = "jdbc:postgresql://localhost/gopal"
properties = {"user": "postgres","driver": "org.postgresql.Driver"}
schemaPeople.write.jdbc(url=url, table="sales", mode=mode, properties=properties)
print("data imported succesfully")


