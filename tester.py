import pyspark
#
# version = "14.2-SNAPHOT"
# spark_jars_packages = ','.join(['org.apache.mahout:mahout-core:14.1',
#                                 'org.apache.mahout:mahout-spark:jar:scala-2.11:14.1'])
#
# jars = ['https://repo1.maven.org/maven2/org/apache/mahout/mahout-spark/14.1/mahout-spark-14.1-scala_2.11.jar',
#         'https://repo1.maven.org/maven2/org/apache/mahout/mahout-core/14.1/mahout-core-14.1-scala_2.11.jar']

jars = ','.join([f'file:///opt/spark/jars/mahout-core-14.2-SNAPSHOT-scala_2.11.jar',
                 f'file:///opt/spark/jars/mahout-core-14.2-SNAPSHOT-scala_2.11.jar',
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

# jvm = sc._gateway._jvm

m = sc._gateway.jvm.org.apache.mahout.math.Matrices().uniformGenerator(1).asFormatString()
## ^^ Method does not exist

m1 = sc._gateway.jvm.org.apache.mahout.math.Matrices().uniformView(3, 3, 1)
print(m1.asFormatString())

m2 = sc._gateway.jvm.org.apache.mahout.math.Matrices().uniformView(3, 3, 1)
print(m2.asFormatString())

print(m1.plus(m2).asFormatString())

### Paralellize a matrix to a DRM
# NOTE: drmParallelize is just a wrapper on drmParallelizeWithRowIndices
# https://github.com/apache/mahout/blob/ad56ec44705e907a3f7f7c9ee6132f83c9d4b74a/core/src/main/scala/org/apache/mahout/math/drm/package.scala#L67

# def drmParallelizeWithRowIndices(m: Matrix, numPartitions: Int = 1)
#       (implicit ctx: DistributedContext): CheckpointedDrm[Int]


def ref_scala_object(object_name):
    clazz = sc._gateway.jvm.java.lang.Class.forName(object_name+"$")
    ff = clazz.getDeclaredField("MODULE$")
    o = ff.get(None)
    return o

obj = ref_scala_object("org.apache.mahout.sparkbindings.SparkEngine")

## Mahout distributed context
# https://github.com/apache/mahout/blob/ad56ec44705e907a3f7f7c9ee6132f83c9d4b74a/engine/spark/src/main/scala/org/apache/mahout/sparkbindings/package.scala#L62

objectName = "org.apache.mahout.sparkbindings"
mc = sc._gateway.jvm.java.lang.Class.forName(objectName+".package$").mahoutSparkContext(
    "local[*]",
    "pymahout-app",
    None,
    sc._conf,
    True
    )

obj2 = ref_scala_object("org.apache.mahout.sparkbindings")


# object_name = "org.apache.mahout.sparkbindings.SparkEngine"
# clazz = sc._gateway.jvm.java.lang.Class.forName(object_name+"$")
# clazz.getDeclaredField()
# drm1= sc._gateway.jvm.().drmParallelizeEmpty(100, 5)
# obj = ref_scala_object(object_name)
obj.getClass().getName() # returns 'org.apache.mahout.sparkbindings.SparkEngine$'
obj2 = ref_scala_object("org.apache.mahout.sparkbindings.SparkEngine")
# obj.drmParallelizeEmpty(100,10) #method does not exist
obj.get("hdfsUtils")
drm2= sc._gateway.jvm.org.apache.mahout.sparkbindings.SparkEngine$drmParallelizeWithRowIndices(m1)
#org.apache.mahout.math.drm.drmParallelize
### Create an RDD
sc._jvm.org.apache.mahout.sparkbindings.SparkEngine.drmParallelizeEmpty(100, 10)
### Wrap the RDD into a DRM

