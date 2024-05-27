import sys
if(len(sys.argv)!=3):
    print("Provide Input File and Output Directory")
    sys.exit(0)

from pyspark import SparkContext
sc =SparkContext()
f = sc.textFile(sys.argv[1])

temp=f.map(lambda x: (x.split(',')[7],1))

data=temp.countByKey()

dd=sc.parallelize(data.items())
dd.saveAsTextFile(sys.argv[2])
