import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import DateType
from pyspark.sql.functions import when
import json
import pyspark.sql.types as T
import datetime
import argparse


if __name__=='__main__':
    sc = pyspark.SparkContext()
    # sc

    spark = SparkSession.builder\
            .master("spark")\
            .appName("Categories")\
            .config('spark.ui.port', '4050')\
            .getOrCreate()

    parser = argparse.ArgumentParser()
    parser.add_argument('OUTPUT_PREFIX', metavar='OUTPUT_PREFIX', type=str)
    args = parser.parse_args()

    OUTPUT_PREFIX = args.OUTPUT_PREFIX

    naics_codes = [452210,452311,445120,722410,722511, 722513,446110, 446191,311811,722515,445210,445220,445230,445291,445292,445299, 445110]
    corePlaces = spark.read.option("header", True).option("inferSchema" , "true").csv("hdfs:///data/share/bdm/core-places-nyc.csv")
    weeklyPatterns = spark.read.option("header", True).option("inferSchema" , "true").csv("hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*")
    filteredCorePlaces = corePlaces.filter(corePlaces.naics_code.isin(naics_codes)).\
    withColumn("file_name", when(F.col("naics_code").isin(452210,452311), "big_box_grocers").\
               when(F.col("naics_code").isin(445120), "convenience_stores").\
               when(F.col("naics_code").isin(722410), "drinking_places").\
               when(F.col("naics_code").isin(722511), "full_service_restaurants").\
               when(F.col("naics_code").isin(722513), "limited_service_restaurants").\
               when(F.col("naics_code").isin(446110, 446191), "pharmacies_and_drug_stores").\
               when(F.col("naics_code").isin(311811,722515), "snack_and_bakeries").\
               when(F.col("naics_code").isin(445210,445220,445230,445291,445292,445299), "specialty_food_stores").\
               when(F.col("naics_code").isin(445110), "supermarkets_except_convenience_stores")).\
    select("placekey","safegraph_place_id","naics_code","file_name")

    def explodeVisits(date_range_start, visit_by_day):
      start = datetime.datetime(*map(int, date_range_start[:10].split('-')))
      return {(start + datetime.timedelta(days=days)):visits for days, visits in enumerate(json.loads(visit_by_day))}
    #Credit to the professor, I levarage this piece of code from class

    udfExpand = F.udf(explodeVisits, T.MapType(DateType(), T.IntegerType()))
    df = spark.read.csv("hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*", header=True) \
           .select("placekey","safegraph_place_id",
              F.explode(udfExpand('date_range_start', 'visits_by_day')) \
                 .alias('date', "visits"))
           # .where(f"date=='{date}'")
    #Credit to the professor, I leverage this piece of code from class


    categories = ["big_box_grocers", "convenience_stores", "drinking_places", "full_service_restaurants","limited_service_restaurants", "pharmacies_and_drug_stores", "snack_and_bakeries", "specialty_food_stores", "supermarkets_except_convenience_stores"]

    for c in categories:
        df.join(filteredCorePlaces, ["placekey"], "inner").groupBy("date","file_name")\
            .agg(F.percentile_approx("visits", 0.5).alias('median'), F.round(F.stddev("visits")).cast("integer").alias('std'))\
            .withColumn("low", when(F.col("std") > F.col("median"), 0).otherwise(F.col("median") - (F.col("std"))))\
            .withColumn("high", F.col("median") + F.col("std"))\
            .withColumn("year", F.year("date"))\
            .withColumn("project_date", F.add_months(F.col("date"), 12))\
            .sort(F.col("year"), F.col("project_date"))\
            .where((F.col("year").isin(2019, 2020)) & (F.col("file_name") == c))\
            .select(F.col("year"), F.col("project_date").alias('date'), F.col("median"),F.col("low"),F.col("high"))\
            .coalesce(1).write.mode("overwrite").option("header",True).format("csv").save("/{}/{}".format(OUTPUT_PREFIX, c))