from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window


sc=SparkContext()

spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")

pro_df = spark.read.format('jdbc').options(
url = "jdbc:postgresql://localhost/gopal?user=postgres", database='gopal',dbtable='product').load()
pro_df.createOrReplaceTempView("product")

sales_df = spark.read.format('jdbc').options(
url = "jdbc:postgresql://localhost/gopal?user=postgres", database='gopal',dbtable='sales').load()
sales_df.createOrReplaceTempView("sales")
sales_df.printSchema()
sales_df.show()

cus_df = spark.read.format('jdbc').options(
url = "jdbc:postgresql://localhost/gopal?user=postgres", database='gopal',dbtable='customer').load()
cus_df.createOrReplaceTempView("customer")

ref_df = spark.read.format('jdbc').options(
url = "jdbc:postgresql://localhost/gopal?user=postgres", database='gopal',dbtable='refund').load()
ref_df.createOrReplaceTempView("refund")

#Display the distribution of sales by product name and product type.

#df=pro_df.join(sales_df,['product_id']).select(pro_df.product_name,pro_df.product_type).distinct()
#df.show()
sqldf=spark.sql("select distinct product_name,product_type from product join sales on product.product_id=sales.product_id")
sqldf.show()

#Find a product that has not been sold at least once (if any).

#df2=sales_df.where(sales_df.total_quantity<1)
#df2.show()
df2=spark.sql("select * from sales where total_quantity<1")
df2.show()

#Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today.

#yearr_df = sales_df.select(year(sales_df.timestamp).alias('year'))

df3 = spark.sql("SELECT count(transaction_id),sum(total_amount) FROM sales where year(timestamp)='2013' and transaction_id not in(select refund.original_transaction_id from refund)")

df3.show()


#Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded

df4=spark.sql("select distinct customer_first_name,count(total_quantity) as purchases from customer,sales where sales.customer_id=customer.customer_id and year(timestamp)='2013' and month(timestamp)='05' and transaction_id not in(select refund.original_transaction_id from refund) group by customer.customer_first_name order by purchases desc")
df4.show(10)
df4.select("customer_first_name").filter("purchases == 3").show()

#Calculate the total number of users who purchased the same product consecutively at least 2 times on a given day.

df5=spark.sql("select distinct count(product_id) as product,count(customer_id) as customer from sales group by DATE(timestamp)")
#df5.show(25)
print("total number of users who purchased the same product consecutively at least 2 times on a given day is: ",df5.count())

































