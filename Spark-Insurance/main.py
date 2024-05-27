import sys
if(len(sys.argv)!=3):
    print("Provide input file and output directory")
    sys.exit(0)
  
from pyspark import SparkContext
sc = SparkContext()
f = sc.textFile(sys.argv[1])

temp = f.map(lambda x: (x.split(',')[16],1))

data = temp.countByKey()

dd = sc.parallelize(data.items())
dd.saveAsTextFile(sys.argv[2])
