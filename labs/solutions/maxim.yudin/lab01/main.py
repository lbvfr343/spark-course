# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import pyspark
from pyspark import SparkContext

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, collect_list, to_json, struct
from pyspark.sql.types import *
from pyspark import Row
import json

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.

def getAllFilmsDataframe():
    ratingSchema = StructType([ \
        StructField("userId", IntegerType(), True), \
        StructField("filmId", IntegerType(), True), \
        StructField("rating", StringType(), True)
    ])
    dfRating = spark.read.csv("/Users/18175411/Documents/npl_education/data/lab01/ml-100k/u.data", schema=ratingSchema,
                              sep='\t')

    dfAllFilmResult = dfRating \
        .groupby(["filmId", "rating"]) \
        .count() \
        .sort(col('filmId'), col('rating'))

    #dfAllFilmResult.show(20, truncate=0)

    return dfAllFilmResult

def getStatisticByFilm(dataframe, filmId):
    dfFilm = dataframe \
        .filter(col('filmId') == str(filmId)) \
        .drop(col('rating')) \
        .groupBy(lit('hist_film')) \
        .agg(collect_list('count').alias('ratings'))

    return dfFilm

def getStatisticAllFilms(dataframe):
    dfFilm = dataframe \
        .groupBy(lit('hist_all'), 'rating') \
        .sum('count').alias('ratings')\
        .sort(col('rating'))\
        .drop(col('rating')) \
        .groupBy(lit('hist_all')) \
        .agg(collect_list('sum(count)').alias('ratings'))
        #.agg(collect_list('countNew').alias('ratings'))

    return dfFilm

def func(x, strJson):


    print(x["filmId"])
    print(x['ratings'])
    for i in x['ratings']:
        print(i)

def calculateOnDataframes(spark):
    dfAllFilmResult = getAllFilmsDataframe()
    # dfAllFilmResult.show(100, truncate=0)

    filmId = 328
    dfFilm = getStatisticByFilm(dfAllFilmResult, filmId)
    # dfFilm.show(20, truncate=0)

    dfAllFilms = getStatisticAllFilms(dfAllFilmResult)
    # dfAllFilms.show(20, truncate=0)

    dfFilmFinal = dfFilm.withColumnRenamed('hist_film', 'filmId')
    # dfFilmFinal = dfFilm.rename(columns={'hist_film': 'filmId'})
    df = dfFilmFinal.union(dfAllFilms)
    df.show(20, truncate=0)
    df.printSchema()

    rdd2 = df.rdd.map(lambda x: (x["filmId"], x["ratings"]))
    strJson = '{\n '
    dataIndex = 0
    elementCount = len(rdd2.collect())
    for element in rdd2.collect():
        dataIndex += 1
        strJson = strJson + '"' + element[0] + '": [\n'
        index = 0
        for i in element[1]:
            index += 1
            if (index < len(element[1])):
                strJson = strJson + str(i) + ",\n"
            else:
                strJson = strJson + str(i) + '\n]\n'
                if (dataIndex < elementCount):
                    strJson = strJson + ',\n'
    strJson += '}'
    print(strJson)

def calculateOnRdd(spark):

    ratingSchema = StructType([ \
        StructField("userId", IntegerType(), True), \
        StructField("filmId", IntegerType(), True), \
        StructField("rating", StringType(), True)
    ])
    rddRatingAll = spark.read.csv("/Users/18175411/Documents/npl_education/lab01/ml-100k/u.data",
                                  schema=ratingSchema,
                                  sep='\t').rdd

    dataByFilm = rddRatingAll \
        .filter(lambda x: 328 == x[1]) \
        .map(lambda x: (x[2], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortByKey() \
        .collect()

    dataAll = rddRatingAll \
        .map(lambda x: (x[2], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortByKey() \
        .collect()

    films = {}
    films['hist_film'] = dataByFilm
    films['hist_all'] = dataAll

    strJson = '{\n '
    dataIndex = 0
    elementCount = len(films)
    for key in films:
        dataIndex += 1
        strJson = strJson + '"' + key + '": [\n'
        index = 0
        for rating in films[key]:
            index += 1
            if (index < len(films[key])):
                strJson = strJson + str(rating[1]) + ",\n"
            else:
                strJson = strJson + str(rating[1]) + '\n]\n'
                if (dataIndex < elementCount):
                    strJson = strJson + ',\n'
    strJson += '}'
    print(strJson)

    with open('lab01.json', 'w') as f:
        f.write(strJson)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    config = SparkConf().setMaster("local[1]").setAppName("test")
    sc = SparkContext(conf=config)

    spark = SparkSession.builder\
        .appName("Python Spark SQL basic example")\
        .config("spark.some.config.option", "some-value")\
        .getOrCreate()

    calculateOnRdd(spark)
    #calculateOnDataframes(spark)