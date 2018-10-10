from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc=SparkContext() 
sqlContext = SQLContext(sc)

lines = sc.textFile("/home/gpurama/Spark_task/spark-test/product.txt")
parts = lines.map(lambda l: l.split("|"))
people = parts.map(lambda p: Row(product_id=int(p[0]), product_name=p[1],product_type=p[2],product_version=p[3],product_price=p[4]))
schemaPeople = sqlContext.createDataFrame(people)
mode = "overwrite"
url = "jdbc:postgresql://localhost/gopal"
properties = {"user": "postgres","driver": "org.postgresql.Driver"}
schemaPeople.write.jdbc(url=url, table="product", mode=mode, properties=properties)
print("data imported succesfully")
