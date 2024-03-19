from pyspark import SparkConf
from pyspark.sql import SparkSession
from util import *

if __name__ == "__main__":

    conf = SparkConf() \
            .setAppName("SparkApp") \
            .setMaster("local[3]")

    spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

    # READ
    survey_df = load_survey_df(spark, "input/sample.csv")

    # # get the number of partitions
    # print(survey_df.rdd.getNumPartitions())

    # re-partition the DataFrame
    repartitioned_survey_df = survey_df.repartition(2)

    # # get the number of partitions
    # print(repartitioned_survey_df.rdd.getNumPartitions())
    #
    # # how many records per partition
    # print(repartitioned_survey_df.rdd.glom().map(len).collect())

    # TRANSFORM
    count_df = count_by_country(repartitioned_survey_df)

    # Write
    # records= count_df.collect()
    # for record in records:
    #     print(record)

    # count_df.show()

    # # save the DataFrame to a single CSV file
    save_count_by_country(count_df, "output/survey_count.csv")

    input("Press Enter to continue...")

    spark.stop()


