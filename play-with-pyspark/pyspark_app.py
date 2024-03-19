
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("HelloSparkSQL") \
    .getOrCreate()    

    surveyDF = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("input/sample.csv")

    # SparkSQL
    surveyDF.createOrReplaceTempView("survey_tbl")

    # SQL Query
    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age < 40 group by Country")

    countDF.show()

    spark.stop()
    
