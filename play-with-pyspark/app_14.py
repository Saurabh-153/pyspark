from pyspark.sql import SparkSession,Window
from pyspark import SparkConf
from pyspark.sql import functions as f
from pyspark.sql.types import *

'''
    Example : Window Aggregations with Dataframe
'''



if __name__ == "__main__":

    conf=SparkConf().setAppName("pyspark-app").setMaster("local[3]")

    # Create a SparkSession
    spark = SparkSession \
        .builder\
        .config(conf=conf) \
        .getOrCreate()
    

    summary_df = spark.read.parquet("source/summary.parquet")

    running_total_window=Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    summary_df.withColumn("RunningTotal",f.sum("InvoiceValue").over(running_total_window)) \
    .show(10)

    
    spark.stop()

