import sys
assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import SparkSession
import datetime

def main(output1, output2, output3):
    a = "/Users/sukhi/Desktop/cmpt353/reddit/trouble_makers"
    b = "/Users/sukhi/Desktop/cmpt353/reddit/contro"
    c = "/Users/sukhi/Desktop/cmpt353/reddit/non_contro"
    t1 = spark.read.json(a)
    t2 = spark.read.json(b)
    t3 = spark.read.json(c)
    t1.coalesce(1).write.json(output1, mode='append')
    t2.coalesce(1).write.json(output2, mode='append')
    t3.coalesce(1).write.json(output3, mode='append')

if __name__ == '__main__':
    output1 = sys.argv[1]
    output2 = sys.argv[2]
    output3 = sys.argv[3]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.4' # make sure we have Sprk 3.4+
    spark.sparkContext.setLogLevel('WARN')
   

    main(output1, output2, output3)