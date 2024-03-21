from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf,expr
from pyspark.sql.types import StringType
import re

'''
    Example : UDF with Dataframe
'''

def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"



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
    
    # Create UDF
    # parse_gender_udf=udf(parse_gender,returnType=StringType()) # HOF
    # survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    # survey_df2.select("Age","Gender").show(truncate=False)

    # Register UDF
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.select("Age","Gender").show(truncate=False)

    list=[r for r in spark.catalog.listFunctions() if "parse_gender_udf" in r.name]
    print(list)



    
    spark.stop()

