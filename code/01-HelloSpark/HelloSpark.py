import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

class Employee:
    pass

e=Employee()



if __name__ == "__main__":
   
    conf = get_spark_app_config()

    spark = (SparkSession 
        .builder 
        .config(conf=conf)
        .getOrCreate())

    survey_raw_df = load_survey_df(spark, "data/sample.csv")
    count_df = count_by_country(survey_df=survey_raw_df)
    count_df.show()

    spark.stop()
