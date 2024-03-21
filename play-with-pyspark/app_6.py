from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

'''
Example : Working with SparkTable
'''

if __name__ == "__main__":
  
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("pyspark-app") \
        .enableHiveSupport() \
        .getOrCreate()
     
    # Read data from Parquet file
    fligthTimeParquetDF= spark.read \
    .format("parquet") \
    .option("path", "source/flight-time.parquet") \
    .option("mode","FAILFAST") \
    .load()

    fligthTimeParquetDF= \
    fligthTimeParquetDF.repartition(1)

    # Transformation




    # Write 

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")
  
    # To use
    # .partitionBy("ORIGIN","OP_CARRER") \
    #  or
    # .bucketBy(5, "OP_CARRIER", "ORIGIN") \
    # .sortBy("OP_CARRIER", "ORIGIN") \

    fligthTimeParquetDF.write \
    .format("json") \
    .mode("overwrite") \
    .partitionBy("OP_CARRIER") \
    .saveAsTable("flight_data_tbl")

    print(spark.catalog.listTables("AIRLINE_DB"))
    



