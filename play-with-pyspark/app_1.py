from pyspark.sql import SparkSession
from pyspark import SparkConf

'''
    Example : Spark Application with Dataframe
'''

if __name__ == "__main__":

    conf=SparkConf().setAppName("pyspark-app").setMaster("local[3]")

    # Create a SparkSession
    spark = SparkSession \
        .builder\
        .config(conf=conf) \
        .getOrCreate()

    # Read
    survey_df = spark.read.csv(
        "source/sample.csv", 
        header=True, 
        inferSchema=True)
    
    # get number of partitions
    print(survey_df.rdd.getNumPartitions())

    # Repartition
    survey_df = survey_df.repartition(2)

    # get number of partitions
    print(survey_df.rdd.getNumPartitions())

    # records count per partition
    print(survey_df.rdd.glom().map(len).collect())

    # Tranformation
    country_count_df=survey_df\
    .filter("Age<40") \
    .select("Age","Country") \
    .groupBy("Country") \
    .count()

    # Write
    country_count_df.show()
    # country_count_df.write.csv("output/country_count", mode="overwrite", header=True)

    input("Press Enter to continue...")

    # Stop the SparkSession
    spark.stop()

