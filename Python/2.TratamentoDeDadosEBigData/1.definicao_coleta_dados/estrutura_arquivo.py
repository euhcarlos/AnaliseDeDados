import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

spark = SparkSession.builder.getOrCreate()

df = spark.read.option('header','true').option('inferSchema','true').csv('clientes_tratados.csv',)

df.take(5)

df.show()

df.printSchema()

from pyarrow import schema
lista_campos = [
    StructField('nome', StringType()),
    StructField('cpf', StringType()),
    StructField('idade', FloatType()),
    StructField('endereco', StringType()),
    StructField('estado',StringType()),
    StructField('pais', StringType()),
    StructField('salario', FloatType()),
    StructField('nivel_educacao', StringType()),
    StructField('numero_filhos', IntegerType()),
    StructField('estado_civil', StringType()),
    StructField('anos_experiencia',IntegerType()),
    StructField('area_atuacao', StringType()),
]
schema_definido = StructType(lista_campos)

df = spark.read.option('header','true').schema(schema_definido).csv('clientes_tratados.csv')

# Criaçãp de dados
dados_teste = [
    Row(nome='Vitor Lima', cpf='612.570.493-99', idade=56.0, endereco='19/01/1968', estado='Estrada Barbosa, 255', pais=None, salario=None, nivel_educacao=None, numero_filhos=None, estado_civil=None, anos_experiencia=None, area_atuacao=None),
    Row(nome='Vila Tirol', cpf=None, idade=None, endereco=None, estado=None, pais=None, salario=None, nivel_educacao=None, numero_filhos=None, estado_civil=None, anos_experiencia=None, area_atuacao=None),
    Row(nome='53133-647 Barros / PR"', cpf='Pará', idade=None, endereco='13550.54', estado='Ensino Médio', pais='0', salario=None, nivel_educacao='17', numero_filhos=None, estado_civil=None, anos_experiencia=None, area_atuacao=None),
    Row(nome='Ana Clara Martins', cpf='648.129.530-06', idade=33.0, endereco='02/12/1990', estado='Favela Duarte, 44', pais=None, salario=None, nivel_educacao=None, numero_filhos=None, estado_civil=None, anos_experiencia=None, area_atuacao=None),
    Row(nome='Bonfim', cpf=None, idade=None, endereco=None, estado=None, pais=None, salario=None, nivel_educacao=None, numero_filhos=None, estado_civil=None, anos_experiencia=None, area_atuacao=None)
  ]

df_parcial = spark.createDataFrame(data=dados_teste, schema=schema_definido)

df_parcial.show()

spark.stop()