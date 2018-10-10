from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc=SparkContext() 
sqlContext = SQLContext(sc)

lines = sc.textFile("/home/gpurama/Spark_task/spark-test/cust.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(customer_id=int(p[0]),customer_first_name=p[1],customer_last_name=p[2],phone_number=int(p[3])))
schemaPeople = sqlContext.createDataFrame(people)
#print(schemaPeople.show())
mode = "overwrite"
url = "jdbc:postgresql://localhost/gopal"
properties = {"user": "postgres","driver": "org.postgresql.Driver"}
schemaPeople.write.jdbc(url=url, table="customer", mode=mode, properties=properties)
print("data imported succesfully")

