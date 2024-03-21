from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f
from pyspark.sql.types import *

'''
    Example : Grouping Aggregations with Dataframe
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



    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    exSummary_df = \
    invoice_df \
    .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
    .where("year(InvoiceDate) == 2010") \
    .withColumn("WeekNumber", f.weekofyear("InvoiceDate")) \
    .groupBy("Country","WeekNumber") \
    .agg(NumInvoices, TotalQuantity, InvoiceValue) \
    .orderBy("Country","WeekNumber") 


    exSummary_df.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("sink/summary")
    
    
    spark.stop()

