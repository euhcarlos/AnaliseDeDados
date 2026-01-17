import pyspark
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("TestApp")
sc = SparkContext(conf=conf).getOrCreate()

rdd = sc.textFile('README.md')

rdd.count()

rdd.take(10)

palavra = rdd.flatMap(lambda x: x.split(' '))
palavra.take(5)

palavraMinuscula = palavra.map(lambda x:x.lower())
palavraMinuscula.take(5)

palavrasMaiscula = palavra.flatMap(lambda x: x.upper())
palavrasMaiscula.take(5)

palavraComecaT = palavraMinuscula.filter(lambda x: x.startswith('t'))
palavraComecaT.take(5)

palavraMaiorQue2 = palavra.filter(lambda x: len(x) > 2)
palavraMaiorQue2.take(5)

palavraChaveValor = palavra.map(lambda x: (x,1))
palavraChaveValor.take(5)

palavraContar = palavraChaveValor.reduceByKey(lambda x,y: x+y)
palavraContarOrd = palavraContar.sortByKey(ascending=-1)
palavraContar.take(5)

palavraContarOrd.saveAsTextFile('palavrasContar')

rddContarPalavra = sc.textFile('palavrasContar')

rddContarPalavra.take(10)