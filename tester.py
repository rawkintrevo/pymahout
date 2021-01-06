import pyspark

version = "14.2-SNAPHOT"
spark_jars_packages = ','.join(['org.apache.mahout:mahout-core:14.1',
                                'org.apache.mahout:mahout-spark:jar:scala-2.11:14.1'])

jars = ['https://repo1.maven.org/maven2/org/apache/mahout/mahout-spark/14.1/mahout-spark-14.1-scala_2.11.jar',
        'https://repo1.maven.org/maven2/org/apache/mahout/mahout-core/14.1/mahout-core-14.1-scala_2.11.jar']

jars = ','.join([f'file:///opt/spark/jars/mahout-core-14.2-SNAPSHOT-scala_2.11.jar',
                 f'file:///opt/spark/jars/apache-mahout-14.2-SNAPSHOT-dependency-reduced.jar'])
spark_conf = pyspark.SparkConf()
spark_conf.setAll([
    ('spark.kryo.referenceTracking','false'),
    ('spark.kryo.registrator', 'org.apache.mahout.sparkbindings.io.MahoutKryoRegistrator'),
    ('spark.kryoserializer.buffer', '32'),
    ('spark.kryoserializer.buffer.max', '600m'),
    ('spark.serializer','org.apache.spark.serializer.KryoSerializer'),
    # ('spark.jars.packages', spark_jars_packages) ,
    ('spark.jars', jars)
    ])

sc = pyspark.SparkContext('local[*]', conf=spark_conf)

jvm = sc._gateway._jvm

m = sc._gateway.jvm.org.apache.mahout.math.Matrices().uniformGenerator(1).asFormatString()
## ^^ Method does not exist

m1 = sc._gateway.jvm.org.apache.mahout.math.Matrices().uniformView(3, 3, 1)
print(m1.asFormatString())

m2 = sc._gateway.jvm.org.apache.mahout.math.Matrices().uniformView(3, 3, 1)
print(m2.asFormatString())

print(m1.plus(m2).asFormatString())

