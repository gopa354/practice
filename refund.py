from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
#import pyspark.sql.functions.unix_timestamp

sc=SparkContext() 
sqlContext = SQLContext(sc)

lines = sc.textFile("/home/gpurama/Spark_task/spark-test/Refund.txt")

parts = lines.map(lambda l: l.split("|"))

people = parts.map(lambda p: Row(refund_id=int(p[0]),original_transaction_id=int(p[1]),customer_id=int(p[2]),product_id=int(p[3]),timestamp=p[4],refund_amount=int(p[5]),refund_quantity=int(p[6])))

schemaPeople = sqlContext.createDataFrame(people)
print(schemaPeople.show())

mode = "overwrite"
url = "jdbc:postgresql://localhost/gopal"
properties = {"user": "postgres","driver": "org.postgresql.Driver"}
schemaPeople.write.jdbc(url=url, table="refund", mode=mode, properties=properties)
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@data imported succesfully")


