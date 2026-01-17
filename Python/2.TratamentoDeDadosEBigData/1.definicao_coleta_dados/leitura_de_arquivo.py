import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('clientes_tratados.csv')

df.take(5)

df.show(5)

df.printSchema()

df = spark.read.option('header', 'true').csv('clientes_tratados.csv')

# Inferir o schema
df = spark.read.option('header', 'true').option('inferSchema', 'true').csv('clientes_tratados.csv')

df = spark.read.\
    option('sep',',').\
    option('header','true').\
    option('inferSchema','true').\
    csv('clientes_tratados.csv')

