from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, 
                               StructField, 
                               StringType, 
                               IntegerType, 
                               DoubleType, 
                               DateType, 
                               TimestampType)


'''
Example : Working with DataFrameReader i.e source & schema
'''

if __name__ == "__main__":
  
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("pyspark-app") \
        .getOrCreate()


    # Define schema for CSV file ( programatically )
    flightTimeSchema = StructType([
        StructField("FL_DATE", DateType(),True),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    # Read data from CSV file
    fligthTimeCsvDF= spark.read \
    .format("csv") \
    .option("header", True) \
    .schema(flightTimeSchema) \
    .option("dateFormat", "M/d/y") \
    .option("path", "source/flight-time.csv") \
    .option("mode","FAILFAST") \
    .load() 

    # Print the schema
    fligthTimeCsvDF.printSchema()

    # Show the data
    fligthTimeCsvDF.show(5)


    # Define schema for CSV file ( DDL )

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""


    # Read data from JSON file
    fligthTimeJsonDF= spark.read \
    .format("json") \
    .schema(flightSchemaDDL) \
    .option("dateFormat", "M/d/y") \
    .option("path", "source/flight-time.json") \
    .option("mode","FAILFAST") \
    .load()

    # Print the schema
    fligthTimeJsonDF.printSchema()

    # Show the data
    fligthTimeJsonDF.show(5)


    # Read data from Parquet file
    fligthTimeParquetDF= spark.read \
    .format("parquet") \
    .option("path", "source/flight-time.parquet") \
    .option("mode","FAILFAST") \
    .load()

    # Print the schema
    fligthTimeParquetDF.printSchema()

    # Show the data
    fligthTimeParquetDF.show(5)