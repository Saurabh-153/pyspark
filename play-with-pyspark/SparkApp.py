from pyspark.sql import SparkSession
from pyspark import SparkConf

if __name__ == "__main__":
    conf = SparkConf() \
        .setAppName("SparkApp") \
        .setMaster("local[3]")

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    # read
    survey_df = spark.read \
    .csv("data/sample.csv", header=True, inferSchema=True)
    # .format("csv") \
    # .option("header", "true") \
    # .option("inferSchema", "true") \
    # .load("data/sample.csv")

    # survey_df.printSchema()
    # survey_df.show()

    # show partitions survey_df
    # print("Partitions: ", survey_df.rdd.getNumPartitions())

    # Transformations
    count_by_country_df=survey_df \
    .filter("Age<40") \
    .select("Age","Gender","Country","state") \
    .groupby("Country") \
    .count()

    # Action
    count_by_country_df.show()



    spark.stop()
