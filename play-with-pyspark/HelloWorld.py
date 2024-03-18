

from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

    # Print the version of Spark
    print("Spark Version: ", spark.version)

    # Stop the SparkSession
    spark.stop()