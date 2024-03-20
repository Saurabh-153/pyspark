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
     
   

    DF=spark.sql("SELECT * FROM AIRLINE_DB.flight_data_tbl")
    DF.show()
  


    spark.stop()



