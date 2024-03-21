from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f
from pyspark.sql.types import *

'''
    Example : Simple Aggregations with Dataframe
'''



if __name__ == "__main__":

    conf=SparkConf().setAppName("pyspark-app").setMaster("local[3]")

    # Create a SparkSession
    spark = SparkSession \
        .builder\
        .config(conf=conf) \
        .getOrCreate()


    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("source/invoices.csv")

    invoice_df.printSchema()

    # Programmatic way 
    invoice_df \
    .select(f.count("*").alias("Count *"),
            f.sum("Quantity").alias("Total Quantity"),
            f.avg("UnitPrice").alias("Avg Price"),
            f.countDistinct("InvoiceNo").alias("Total Invoice Count"),
            ) \
    .show(truncate=False)
    

    # SQL way
    invoice_df \
    .selectExpr("count(1) as `Count 1`",
                "sum(Quantity) as `Total Quantity`",
                "avg(UnitPrice) as `Avg Price`",
                "count(distinct InvoiceNo) as `Count Distinct InvoiceNo`") \
    .show(truncate=False)


    invoice_df.createOrReplaceTempView("sales")

    # SQL way on Table/View
    summary_sql = spark.sql("""
          SELECT Country, InvoiceNo,
                sum(Quantity) as TotalQuantity,
                round(sum(Quantity*UnitPrice),2) as InvoiceValue
          FROM sales
          GROUP BY Country, InvoiceNo""")
    
    summary_sql.show(truncate=False)

    # Programmatic way on Table/View
    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
             )
    
    summary_df.show()





    spark.stop()

