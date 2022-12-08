import os
import shutil
from pyspark import SparkContext

#Mac
#os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
#os.environ['JAVA_HOME']='/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home'
#os.environ['JRE_HOME']='/Library/java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home/jre/'


#Windows
#os.environ['PYSPARK_PYTHON']='C:/Users/Administrator/.conda/envs/db2/python.exe'
#os.environ['JAVA_HOME']='C:/Program Files/Java/jdk1.8.0_152'
#os.environ['JRE_HOME']='C:\Program Files\Java\jdk1.8.0_152\jre'

#Linux
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
os.environ['JAVA_HOME']='/home/prai/programs/jdk-18.0.2.1'
# os.environ['JRE_HOME']='/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre'




def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )



## MapReduce Framework
def initialise(sc, inputFile, prepare):
    """Open a file and apply the prepare function to each line"""
    input = sc.textFile(inputFile)
    return input.map(prepare)

def finalise(data, outputFile):
    """store data in given file"""
    data.saveAsTextFile(outputFile)


class Mapper:
    def __init__(self):
        self.out = []

    def emit(self, key, val):
        self.out.append((key, val))

    def result(self):
        return self.out


class Reducer:
    def __init__(self):
        self.out = []

    def emit(self, key, val):
        self.out.append((key, val))

    def result(self):
        return self.out


def transform(input, mapper, reducer):
    """map reduce framework"""
    return input.flatMap(lambda kd: mapper().map(kd[0], kd[1]).result())\
	.groupByKey() \
	.flatMap(lambda kd: reducer().reduce(kd[0], kd[1]).result())

### End of MapReduce Framework


##  Complete Example ##


##Mapper implementation
class WCMapper(Mapper):
	# required
	def __init__(self):
		super().__init__()
	# provided by YOU
	def map(self, key, data):
		for x in data.split(" "):
			self.emit(x, 1)
		return self
		
##Reducer implementation
class WCReducer(Reducer):
	# required
	def __init__(self):
		super().__init__()
	
	def reduce(self, key, datalist):
		self.emit(key, sum(datalist))
		return self

def wordcount(sc, inputFile, outputFile):
	if os.path.isdir(outputFile):
		shutil.rmtree(outputFile)
	rdd = initialise(sc, inputFile, lambda line: ("NoKey", line))
	result = transform(rdd, WCMapper, WCReducer)
	# passing class NOT object (which would be WCMapper() etc)
	finalise(result, outputFile)


#####################################

############# Question 1 ############

##Mapper implementation
class Q1Mapper(Mapper):
	# required
	def __init__(self):
		super().__init__()
		
	def map(self, key, data):
		#fill_in
		k=3
		for i in range(0, len(data), k):
			self.emit(data[i:i + k], 1)
		return self
		
##Reducer implementation
class Q1Reducer(Reducer):
	# required
	def __init__(self):
		super().__init__()
		
	def reduce(self, key, datalist):
		self.emit(key, sum(datalist))
		return self

def q1(sc, inputFile, outputFile):
	if os.path.isdir(outputFile):
		shutil.rmtree(outputFile)
	rdd = initialise(sc, inputFile, lambda line: ("NoKey", line))
	result = transform(rdd, Q1Mapper, Q1Reducer)
	finalise(result, outputFile)

#####################################


############# Question 2 ############

##Mapper implementation
class Q2Mapper(Mapper):
	# required
	def __init__(self):
		super().__init__()
		
	def map(self, key, data):
		#fill_in
		splitted=data.split("|")
		name=splitted[0]
		coutry_code = splitted[1]
		province = splitted[2]
		population = splitted[3]
		if population != "" and float(population) < float(100000):
			self.emit((name, coutry_code, province), float(population))
		return self
		
##Reducer implementation
class Q2Reducer(Reducer):
	# required
	def __init__(self):
		super().__init__()
		
	def reduce(self, key, datalist):
		self.emit(key, sum(datalist))
		return self

def q2(sc, inputFile, outputFile):
	if os.path.isdir(outputFile):
		shutil.rmtree(outputFile)
	rdd = initialise(sc, inputFile, lambda line: ("NoKey", line))
	result = transform(rdd, Q2Mapper, Q2Reducer)
	finalise(result, outputFile)

#####################################


############# Question 3 ############

##Mapper implementation
class Q3Mapper(Mapper):
	# required
	def __init__(self):
		super().__init__()
		
	def map(self, key, data):
		#fill_in
		splitted=data.split("|")
		name=splitted[0]
		coutry_code = splitted[1]
		province = splitted[2]
		population = splitted[3]
		if population != "":
			self.emit((coutry_code, province), float(population))
		return self
		
##Reducer implementation
class Q3Reducer(Reducer):
	# required
	def __init__(self):
		super().__init__()
		
	def reduce(self, key, datalist):
		self.emit(key, sum(datalist))
		return self

def q3(sc, inputFile, outputFile):
	if os.path.isdir(outputFile):
		shutil.rmtree(outputFile)
	rdd = initialise(sc, inputFile, lambda line: ("NoKey", line))
	result = transform(rdd, Q3Mapper, Q3Reducer)
	finalise(result, outputFile)

#####################################

sc = SparkContext("local", "Simple App")
quiet_logs(sc)



wordcount(sc, "lorem.txt", "wordcount.out")
# q1(sc,"sequence.txt","q1.out")
# q2(sc,"City.dat","q2.out")
# q3(sc,"City.dat","q3.out")



