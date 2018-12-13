import sys
from pyspark import SparkContext
import csv


sc=SparkContext(appName='inf553')
users=sc.textFile(sys.argv[1])
ratings=sc.textFile(sys.argv[2])
movies=sc.textFile(sys.argv[3])

##### mapper#####
users=users.map(lambda x:x.split('::')).map(lambda x:(x[0],x[1]))
ratings=ratings.map(lambda x:x.split('::')).map(lambda x:(x[0],(x[1],x[2])))
movies=movies.map(lambda x:x.split('::'))
genre=movies.map(lambda x:(x[0],x[2]))
join=users.join(ratings)
join=join.map(lambda x:(x[1][1][0],(x[1][0],x[1][1][1])))
join=join.join(genre)
join=join.map(lambda x:((x[1][0][0],x[1][1]),int(x[1][0][1])))

##### reducer#####
rddSumOfRating=join.aggregateByKey((0,0), lambda a,b: (a[0] + b,    a[1] + 1),
                                       lambda a,b: (a[0] + b[0], a[1] + b[1]))
rddAvg=rddSumOfRating.map(lambda x:(x[0],x[1][0]/x[1][1])).sortBy(lambda x:x[0][1])
output=rddAvg.collect()
fileToOpen=open(sys.argv[4],'w')
csvCursor = csv.writer(fileToOpen)
header = ['genre','gender','ratings']
csvCursor.writerow(header)

#print output
for v in output:
    data = ['%s' % (v[0][1]), '%s' % (v[0][0]),'%s' % (v[1])]
    csvCursor.writerow(data)

fileToOpen.close()

