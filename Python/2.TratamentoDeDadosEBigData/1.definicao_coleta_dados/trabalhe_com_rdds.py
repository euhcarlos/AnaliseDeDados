import pyspark
from pyspark import SparkContext

sc = SparkContext.getOrCreate() # Início

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

rdd.getNumPartitions()

rdd.take(3)

rdd.collect() # Cuidado

rddTxt = sc.parallelize(['Spark','Carlos','Dados','Hadoop', 'Python'])

def funcMin(palavra):
    palavra = palavra.lower()
    return palavra

rddTextoMinusculo = rddTxt.map(funcMin)
rddTextoMinusculo.take(3)

rddTextoMinusculo1 = rddTxt.map(lambda x: x.lower())
rddTextoMinusculo1.take(3)

sc.stop() # Fim