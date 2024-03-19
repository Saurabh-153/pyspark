def load_survey_df(spark, data_file):
    # Load the DataFrame
    survey_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(data_file)
    return survey_df


def count_by_country(survey_df):
    count_df = survey_df \
        .filter("Age < 40") \
        .select("Country") \
        .groupBy("Country") \
        .count()
    return count_df


def save_count_by_country(count_df, output_file):
    count_df \
        .coalesce(1) \
        .write \
        .format("csv") \
        .option("header", "true") \
        .save(output_file)
