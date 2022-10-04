# Подключение к JupyterHub

В этом документе вы узнаете, как работать с кластером через браузер в JupyterHub используя Apache Spark.

Для работы с Apache Spark мы будем пользоваться JupyterHub. Это обычный Jupyter Notebook только для многопользовательского режима.

Для подключения к нему достаточно ввести свой логин от ЛК и пароль на той мастер-ноде, к которой относитесь вы:

- [https://spark-master-4.newprolab.com](https://spark-master-4.newprolab.com)
- [https://spark-master-5.newprolab.com](https://spark-master-5.newprolab.com)

## Для получения спарк сессии достаточно исполнить

```python
import os
import sys
os.environ["PYSPARK_PYTHON"]='/opt/anaconda/envs/bd9/bin/python'
os.environ["SPARK_HOME"]='/usr/hdp/current/spark2-client'
os.environ["PYSPARK_SUBMIT_ARGS"]='--num-executors 2 pyspark-shell'

spark_home = os.environ.get('SPARK_HOME', None)
if not spark_home:
    raise ValueError('SPARK_HOME environment variable is not set')

sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))
exec(open(os.path.join(spark_home, 'python/pyspark/shell.py')).read())
```

Вы увидите

```bash
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.7
      /_/

Using Python version 3.6.5 (default, Apr 29 2018 16:14:56)
SparkSession available as 'spark'.

```

## Настройка спарк сессии
```python
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import Row
import json

conf = SparkConf()

spark = (SparkSession
         .builder
         .config(conf=conf)
         .appName("test")
         .getOrCreate())
```


## Информация о спарк сессии

```python
spark
```

Вы увидите

```bash
SparkSession - in-memory

SparkContext

Spark UI

Version
    v2.4.7
Master
    yarn
AppName
    pyspark-shell

```

## После завершения работы правильно закрыть контекст

```python
spark.stop()
```
