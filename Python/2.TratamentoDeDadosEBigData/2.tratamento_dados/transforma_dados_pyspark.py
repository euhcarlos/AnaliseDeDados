import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,upper
from pyspark.sql.functions import to_date, date_format, unix_timestamp, from_unixtime

spark = SparkSession.builder.getOrCreate()

vendedores = spark.read.csv("drive/MyDrive/Spark/Data/vendedores.csv", header=True, inferSchema=True)
itens_pedidos = spark.read.csv("drive/MyDrive/Spark/Data/itens_pedido.csv", header=True, inferSchema=True)

vendedores.show()
itens_pedidos.show()

vendedores_tratados_df = vendedores.withColumn('cep_vendedor', vendedores.cep_vendedor.cast('string'))
vendedores_tratados_df = vendedores_tratados_df.withColumn('cidade_vendedor_tratado', upper(vendedores['cidade_vendedor']))

vendedores_tratados_df.show(5)
vendedores_tratados_df.printSchema()

itens_pedidos.show(2)
itens_pedidos.printSchema()

itens_pedidos_tratados_df = itens_pedidos.\
  withColumn('preco', col('preco').cast('float')).\
  withColumn('valor_frete', col('valor_frete').cast('float')).\
  withColumnRenamed('valor_frete', 'frete').\
  withColumn('valor_total', col('preco') + col('frete'))

itens_pedidos_tratados_df.show(5)
itens_pedidos_tratados_df.printSchema()

itens_pedidos_data_df = itens_pedidos_tratados_df.withColumn('data', to_date(col('data_limite_envio')))
itens_pedidos_data_df = itens_pedidos_data_df.withColumn('data_hr', date_format(col('data_limite_envio'), 'dd\MM\yyyy'))
itens_pedidos_data_df = itens_pedidos_data_df.withColumn('hora', date_format(col('data_limite_envio'), 'HH:mm:ss'))

itens_pedidos_data_df.show(5)
itens_pedidos_data_df.printSchema()

itens_pedidos_data_reverso_df = itens_pedidos_data_df.withColumn('timestamp',unix_timestamp(col('data_hr'),'dd\\MM\\yyyy'))
itens_pedidos_data_reverso_df = itens_pedidos_data_reverso_df.withColumn('data_formatada', from_unixtime('timestamp','yyyy-MM-dd')).withColumn('data_formatada',col('data_formatada').cast('date'))

itens_pedidos_data_reverso_df.show(5)
itens_pedidos_data_reverso_df.printSchema()

itens_pedidos_data_df.write.mode('overwrite').option('header','true').csv('drive/MyDrive/Spark/Data/output/itens_pedido_tratado.csv')

spark.read.csv('drive/MyDrive/Spark/Data/output/itens_pedido_tratado.csv', header=True, inferSchema=True).show()

spark.stop()