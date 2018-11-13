# Mavel-Names: (heroID, superheroName)
# Marvel-Graphs: (heroID, all associatedhero ID)
# Find the hero that has the most connections with other superheros
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def countCoOccurence(line):
	fields = line.split()
	return ( int(fields[0]), len(fields[1:])-1 )

def parseNames(names):
	nameList = names.split("\"")
	return ( (int(nameList[0]), nameList[1].encode("utf8")))
	
names = sc.textFile("marvel-names.txt")
namesRdd = names.map(parseNames) # (id, name)

lines = sc.textFile("marvel-graph.txt")
occurence = lines.map(countCoOccurence).reduceByKey(lambda x,y:x+y) #(heroID, occurence)
# sorted to find the the max occurence
mostPopular = occurence.map(lambda x: (x[1],x[0])).max() #(occurence, heroID)
#find the hero name associated with ID
mostPopularWithName = namesRdd.lookup(mostPopular[1])[0]
# print(occurenceWithName)
print(str(mostPopularWithName) + " is the most popular superhero, with " + \
		str(mostPopular[0])+ " co-appearances.")
