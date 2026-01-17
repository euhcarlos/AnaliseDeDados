import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.option('header','true').csv('clientes_tratados.csv')

df.show()

df.write.csv('output/clientes_csv')

df.write.mode('overwrite').option('header','true').csv('output/clientes_csv')

df = spark.read.option('header','true').option('inferSchema','true').csv('output/clientes_csv')

df.show()

df.write.option('header','true').json('output/clientes_json')
df.show()

df.write.option('header','true').parquet('output/clientes_parquet')
df.show()

df.write.option('header','true').saveAsTable('cliente')

df.createOrReplaceGlobalTempView('nome')
df.show()

spark.catalog.listaDatabases()

tab_df = spark.sql('select * from cliente')
tab_df.show()

spark.stop()