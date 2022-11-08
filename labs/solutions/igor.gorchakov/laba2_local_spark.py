# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 22:41:28 2022

@author: Igor
"""


import os
import sys
from os.path import join
# import pandas as pd
import json

os.environ["SPARK_HOME"]=r'C:\spark-3.3.0-bin-hadoop3'
os.environ["HADOOP_HOME"]=r'C:\spark-3.3.0-bin-hadoop3'
os.environ['PYSPARK_PYTHON'] = sys.executable 
# os.environ["PYSPARK_PYTHON"]='/opt/anaconda/envs/bd9/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f_

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover #,VectorAssembler
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
import json

#%%
path = r'DO_record_per_line.json' # /labs/slaba02/DO_record_per_line.json

conf = (SparkConf().setMaster('local') ) # local[*]
spark = SparkSession.builder.appName("LABA2").config(conf=conf).getOrCreate()

df = spark.read.json(path)
df.show(10)
df.count()

courses_list = [[23126, u'en', u'Compass - powerful SASS library that makes your life easier'], [21617, u'en', u'Preparing for the AP* Computer Science A Exam \u2014 Part 2'], [16627, u'es', u'Aprende Excel: Nivel Intermedio by Alfonso Rinsche'], [11556, u'es', u'Aprendizaje Colaborativo by UNID Universidad Interamericana para el Desarrollo'], [16704, u'ru', u'\u041f\u0440\u043e\u0433\u0440\u0430\u043c\u043c\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u043d\u0430 Lazarus'], [13702, u'ru', u'\u041c\u0430\u0442\u0435\u043c\u0430\u0442\u0438\u0447\u0435\u0441\u043a\u0430\u044f \u044d\u043a\u043e\u043d\u043e\u043c\u0438\u043a\u0430']]

pc_ids=[x[0] for x in courses_list]
langList = ['en','es','ru']
# with open(path, "r", encoding = "ANSI") as f:
#     data = json.load(f)

# spark = SparkSession.builder.config(conf=conf).appName("Sergey Grishaev Spark Dataframe app").getOrCreate()

# df.select(df.cat, df.desc).show(5)
# df_en = df.where("lang = 'en'")
# df_en.show(5)
# df_en.count()

# df_es = df.where("lang = 'es'")
# df_es.show(5)
# df_es.count()


# df_ru = df.where("lang = 'ru'")
# df_ru.show(5)
# df_ru.count()
#%%
@f_.udf(FloatType())
def cosine_similarity(v, u):
    return float(v.dot(u) / (v.norm(2) * u.norm(2)))

regexp_clear = f_.lower(f_.regexp_replace('desc',r'[^\pL0-9\p{Space}]','' ))
stop_words = list(set(StopWordsRemover.loadDefaultStopWords("english") + StopWordsRemover.loadDefaultStopWords("russian") + StopWordsRemover.loadDefaultStopWords("spanish")))
len(stop_words)

#%%
data = spark.read.json(path)
# data = spark.read.json('/labs/slaba02/DO_record_per_line.json')
data.show(10)

data2 = data.select(*data.columns ,regexp_clear.alias('desc2')).drop('desc')
data2.show(10)

#%%
tokenizer = Tokenizer(inputCol = "desc2", outputCol = "words")
stopwordsremover = StopWordsRemover(inputCol = "words", outputCol = "words_censored", stopWords = stop_words)
tf = HashingTF(inputCol = "words_censored", outputCol="tf")
# hashingTF = HashingTF(numFeatures = 10000, inputCol = "words", outputCol="tf")
tfidf = IDF(inputCol = "tf", outputCol = "idf")

pipeline = Pipeline(stages=[tokenizer, stopwordsremover, tf, tfidf ])
pipeline_fit = pipeline.fit(data2)
tfidf_data = pipeline_fit.transform(data2)
tfidf_data.show(10)

#%%
cross = tfidf_data.alias('a').crossJoin(tfidf_data.where(f_.col('id').isin(pc_ids)).alias('b'))\
.select('a.*', f_.col('b.id').alias('pc_id'), f_.col('b.lang').alias('pc_lang'), f_.col('b.name').alias('pc_name'), f_.col('b.idf').alias('pc_tfidf') )\
.filter(''' a.lang=pc_lang and pc_id !=id ''')
cross.show(5)

cross2 = cross.select('*',cosine_similarity('idf','pc_tfidf').alias('cos')).filter(''' cos !='NaN' ''')
cross2.show(5)

cross3 = cross2.select('*', f_.row_number().over(Window.partitionBy("pc_id")\
.orderBy(f_.col('cos').desc(), f_.col('name'), f_.col('id'))).alias('n'))\
.filter('n<=10')
cross3.show(5)

#%%
recomendation = cross3.groupBy(f_.col('pc_id')).agg(f_.collect_list(f_.col('id')).alias('top_10_ids')).orderBy('pc_id').collect()
res = {}
for r in recomendation:
    res[str(r[0])] = r[1]
    print(str(r[0]),':',r[1])

with open('lab02.json', 'w') as f:
    json.dump(res, f, indent=3)

spark.stop()
