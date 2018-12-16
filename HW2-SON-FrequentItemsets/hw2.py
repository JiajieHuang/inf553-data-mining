import sys
from pyspark import SparkContext
import csv
from itertools import combinations




def generateCandidate(frequentSets,k): 
    '''
    given the frequentSets of size k-1, generate 
    the candidate sets of size k.
    input: frequentSets: 
            a list of set with each user's movie
    represented as a set.
    return: candidates:
            a set of candidate sets that could possibl
            be frequent
    ''' 
    if k==2:
        candidates=set(combinations(frequentSets,k))
    else:
        elements=set()
        for fset in frequentSets:
            for item in fset:
                elements.add(item)
        candidates=set(combinations(elements,k))
    return candidates

def generateFrequent(candidates,supp,chunkSet):
    '''
        given the candidates set, the support in the chunk, and the chunk data,
        filter out all the sets from the candiates that are really frequent.
        algorithm: for each candidate, see how many times it has been a subset of 
                    transaction, if this count surpass the support threashold, then
                    add it to the frequent set.
        input: candidates: a set of frequent sets.
        output: frequent: the sets that has a count>= supp
    '''
    countDict={}
    frequent=set()
    for candidate in candidates:
        for setRecord in chunkSet:
            setCandidate=set(candidate)
            if setCandidate.issubset(setRecord):
               if  tuple(setCandidate) not in countDict.keys():
                  countDict[tuple(setCandidate)]=0
               countDict[tuple(setCandidate)]+=1
    for candidate in countDict:
        if countDict[candidate]>=supp:
            frequent.add(candidate)
    return frequent

def preprocessingChunk(chunk):
   '''
   traverse all the transactions in the chunk and get the initialized size=1 
   frequent sets, chunk date with all the users collection converted to set 
   and the support used for this chunk in the SON algorithm.
   input: chunk an iterator containing a division of data
   return:
         frquentSets: a set containing the initialized size=1 frequent sets
         chunkSet: a new chunk data with every record of user data converted
         into a set.
         supportChanged:the support threashold used for this chunk of data.
   '''
   countDict={}
   num=0
   frequentSets=set()
   chunkSet=[]
   for transaction in chunk:          ##traverse the chunk
       num=num+1                      ## add 1 for one record.
       setTransaction=set(transaction) 
       chunkSet.append(setTransaction)
       for item in transaction:       ## traverse the item in the record, and add count 1 for each item
           if item not in countDict.keys():
               countDict[item]=0
           countDict[item]+=1
   supportChanged=num/n*support      
   for item in countDict:             ##filter out the frequent items
       if countDict[item]>=supportChanged:
           frequentSets.add(item)
   return frequentSets,chunkSet,supportChanged
   
def Apriori(chunk):
    '''
        run Apriori on every chunk of data and get the frequent set on the 
        chunk(if a set is frequent globally, it must be frequent on one chunk)
        input: a chunk of transactions.
        output: a iterator containing all the frequent sets of this chunk
    '''
    frequentSets,chunkSet,supportChanged=preprocessingChunk(chunk)
    k=2
    result=[(item,) for item in frequentSets]
    while(len(frequentSets)!=0):
        candidate=generateCandidate(frequentSets,k)
        frequentSets=generateFrequent(candidate,supportChanged,chunkSet)
        if len(frequentSets)>0:
            for frequentSet in frequentSets:
                result.append(frequentSet)
        k=k+1
    return iter(result) 

def generateBasket(x):
    basket=[]
    for item in x[1]:
        basket.append(item[0])
    return basket

def sortKey(x):
    return len(x)+sum(x)/100000

sc=SparkContext(appName='inf553')
users=sc.textFile('users.dat').map(lambda x:x.split("::"))
movies=sc.textFile('movies.dat').map(lambda x:x.split("::"))
ratings=sc.textFile('ratings.dat').map(lambda x:x.split("::"))
case=int(sys.argv[3])
support=int(sys.argv[4])

if case==1:
    male=users.filter(lambda x:x[1]=='M').map(lambda x:(int(x[0]),x[1]))
    ratings=ratings.map(lambda x:(int(x[0]),int(x[1])))
    join=ratings.join(male)
    join=join.groupByKey().map(lambda x:(int(x[0]),list(x[1])))
    baskets=join.map(generateBasket)
    n=baskets.count()

elif case==2:
    female=users.filter(lambda x:x[1]=='F').map(lambda x:(int(x[0]),x[1]))
    ratings=ratings.map(lambda x:(int(x[0]),int(x[1])))
    join=ratings.join(female)
    baskets=join.map(lambda x:(x[1][0],x[0])).groupByKey().map(lambda x:list(x[1]))
    n=baskets.count()


candidate=baskets.mapPartitions(Apriori).map(lambda x:(x,1)).reduceByKey(lambda x,y:x).map(lambda x:x[0])
candidate=sc.broadcast(candidate.collect())
frequent=baskets.flatMap(lambda x:([(count,1) for count in candidate.value if set(count).issubset(set(x))]))
frequent=frequent.reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>=support).map(lambda x:tuple(sorted(x[0])))
frequent=frequent.collect()
length=len(frequent)
frequent.sort(key=sortKey)
##Write the answer to the output file 
op=open(sys.argv[5],'w')
for i in range(length):
    if len(frequent[i])>len(frequent[i-1]):
        op.write("\n")
    if len(frequent[i])==1:
        op.write("("+str(frequent[i][0])+")")
    else:
        op.write(str(frequent[i]))
op.close()
    
