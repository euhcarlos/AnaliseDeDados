import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

produtos = spark.read.csv('drive/MyDrive/Spark/Data/produtos.csv', header=True, inferSchema=True)
vendedores = spark.read.csv('drive/MyDrive/Spark/Data/vendedores.csv', header=True, inferSchema=True)
clientes = spark.read.csv('drive/MyDrive/Spark/Data/clientes.csv', header=True, inferSchema=True)
itens_pedidos = spark.read.csv('drive/MyDrive/Spark/Data/itens_pedido.csv', header=True, inferSchema=True)
pagamentos_pedido = spark.read.csv('drive/MyDrive/Spark/Data/pagamentos_pedido.csv', header=True, inferSchema=True)
avaliacoes_pedido = spark.read.csv('drive/MyDrive/Spark/Data/avaliacoes_pedido.csv', header=True, inferSchema=True)
pedidos = spark.read.csv('drive/MyDrive/Spark/Data/pedidos.csv', header=True, inferSchema=True)

from os import truncate
print("DataFrame Produtos:", produtos.show(n = 5, truncate = False))
print("DataFrame Vendedores:", vendedores.show(5))
print("DataFrame Clientes:", clientes.show(5));
print("DataFrame Itens Pedidos:", itens_pedidos.show(5));
print("DataFrame Pagamentos Pedido:", pagamentos_pedido.show(5));
print("DataFrame Avaliacoes Pedido:", avaliacoes_pedido.show(5));

# Acessar colunas
clientes.select('id_cliente').show(1, truncate=False)
clientes.select(col('id_cliente')).show(1, truncate=False)
clientes.select(clientes['id_cliente']).show(1, truncate=False)
clientes.select(clientes.id_cliente).show(1, truncate=False)

produtos_tratar_nul = produtos.na.fill({'categoria_produto':'Não indentificado'})
produtos_tratar_nul.filter(col('categoria_produto') == 'Não indentificado').count()



from re import sub
print("Total pedidos", pedidos.count())

pedidos_unicos = pedidos.dropDuplicates()
print("Total pedidos únicos", pedidos_unicos.count())

pedidos_remocao_nulos = pedidos_unicos.na.drop()
print("Total pedidos removendo nulos", pedidos_remocao_nulos.count())

pedidos_nulos_id = pedidos_unicos.na.drop(subset=['id_cliente','id_pedido'])
print("Total pedidos removendo nulos id", pedidos_nulos_id.count())

colunas = ['peso_produto_g','comprimento_produto_cm','altura_produto_cm','largura_produto_cm']

for coluna in colunas:
  produtos.na.fill({coluna:0})

produtos.write.mode('overwrite').option('header','true').csv('drive/MyDrive/Spark/Data/output/produtos_tratados.csv')

spark.read.csv('drive/MyDrive/Spark/Data/output/produtos_tratados.csv', header=True, inferSchema=True).show(5)

spark.stop()