from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

'''
Example : Working with DataFrameWriter i.e sink 
'''

if __name__ == "__main__":
  
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("pyspark-app") \
        .getOrCreate()
     
    # Read data from Parquet file
    fligthTimeParquetDF= spark.read \
    .format("parquet") \
    .option("path", "source/flight-time.parquet") \
    .option("mode","FAILFAST") \
    .load()

    print("Num of partitions before: ", fligthTimeParquetDF.rdd.getNumPartitions())
    fligthTimeParquetDF.groupBy(spark_partition_id()).count().show()

    # Repartition
    fligthTimeParquetDF = fligthTimeParquetDF.repartition(2)

    print("Num of partitions after: ", fligthTimeParquetDF.rdd.getNumPartitions())
    fligthTimeParquetDF.groupBy(spark_partition_id()).count().show()


    # Transformation


    


    # Write 
    # fligthTimeParquetDF.show()
    # repartition by coalesce
    fligthTimeParquetDF= fligthTimeParquetDF.coalesce(1)

    fligthTimeParquetDF.write \
    .format("json") \
    .mode("overwrite") \
    .option("path", "sink/json") \
    .option("maxRecordsPerFile", 10000) \
    .option("dateFormat", "yyyy-MM-dd") \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ") \
    .partitionBy("OP_CARRIER","ORIGIN") \
    .save()



