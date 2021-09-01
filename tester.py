import pyspark

jars = ','.join([f'file:///opt/spark/jars/mahout-core-14.2-SNAPSHOT-scala_2.11.jar',
                 f'file:///opt/spark/jars/mahout-core-14.2-SNAPSHOT-scala_2.11.jar',
                 f'file:///opt/spark/jars/apache-mahout-14.2-SNAPSHOT-dependency-reduced.jar',
                 f'file:///opt/spark/jars/mahout-spark-14.2-SNAPSHOT-scala_2.11.jar',])

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

# mc = sc._gateway.jvm.org.apache.mahout.math.Matrices().uniformGenerator(1).asFormatString()
## ^^ Method does not exist

"""
in core
"""
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


## Mahout distributed context
mc = sc._gateway.jvm.org.apache.mahout.sparkbindings.SparkDistributedContext(sc._jsc.sc())


# Parallelize DRM
obj = ref_scala_object("org.apache.mahout.sparkbindings.SparkEngine")
obj.drmParallelizeWithRowIndices(m1)
# object_name = "org.apache.mahout.sparkbindings.SparkEngine"
# clazz = sc._gateway.jvm.java.lang.Class.forName(object_name+"$")
# clazz.getDeclaredField()
# drm1= sc._gateway.jvm.().drmParallelizeEmpty(100, 5)
# obj = ref_scala_object(object_name)
obj.getClass().getName() # returns 'org.apache.mahout.sparkbindings.SparkEngine$'
obj2 = ref_scala_object("org.apache.mahout.sparkbindings.SparkEngine")
# obj.drmParallelizeEmpty(100,10) #method does not exist
obj.get("hdfsUtils")
drm= sc._gateway.jvm.org.apache.mahout.sparkbindings.SparkEngine$drmParallelizeWithRowIndices(m1)
#org.apache.mahout.math.drm.drmParallelize
### Create an RDD
sc._jvm.org.apache.mahout.sparkbindings.SparkEngine.drmParallelizeEmpty(100, 10)
### Wrap the RDD into a DRM


"""out of core"""

# Returns the static object instance on the heap

def ref_scala_object(object_name):
  clazz = sc._gateway.jvm.java.lang.Class.forName(object_name+"$")
  ff = clazz.getDeclaredField("MODULE$")
  o = ff.get(None)
  return o


scala_none = ref_scala_object("scala.None")
scala_none.getClass().getName()


#sdc = sc._gateway.jvm.org.apache.mahout.sparkbindings.SparkDistributedContext(sc)
"""
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/usr/local/lib/python3.7/site-packages/py4j/java_gateway.py", line 1516, in __call__
    [get_command_part(arg, self._pool) for arg in new_args])
  File "/usr/local/lib/python3.7/site-packages/py4j/java_gateway.py", line 1516, in <listcomp>
    [get_command_part(arg, self._pool) for arg in new_args])
  File "/usr/local/lib/python3.7/site-packages/py4j/protocol.py", line 298, in get_command_part
    command_part = REFERENCE_TYPE + parameter._get_object_id()
AttributeError: 'SparkContext' object has no attribute '_get_object_id'
"""

ref_scala_object("org.apache.mahout.sparkbindings")
>>>>>>> f8f7c4397d605fef8f993ce879c3e43ed7e0c3e7
