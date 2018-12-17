import heapq
from math import sqrt
from itertools import combinations
import sys

class Cluster:
    centroid=[]
    count=0
    points=[]
    category=''
    countMisclasified=0
    def __init__(self,points,count,centroid):
        self.points=points
        self.count=count
        self.centroid=centroid

def cluster_distance(c1,c2):
    dist=distance(c1.centroid,c2.centroid)
    return dist

def vecAdd(v1,v2):
    k=len(v1)
    ret=[0]*k
    for i in range(k):
        ret[i]=v1[i]+v2[i]
    return ret

def distance(v1,v2):
    k=len(v1)
    dist=0
    for i in range(k):
        dist+=(v1[i]-v2[i])*(v1[i]-v2[i])
    dist=sqrt(dist)
    return dist

def vecMultiple(v1,times):
    k=len(v1)
    ret=[0]*k
    for i in range(k):
        ret[i]=v1[i]*times
    return ret


def merge(cluster1,cluster2):
    points=cluster1.points+cluster2.points
    count=cluster1.count+cluster2.count
    centroid=vecAdd(vecMultiple(cluster1.centroid,cluster1.count/count),vecMultiple(cluster2.centroid,cluster2.count/count))
    return Cluster(points,count,centroid)

def getKind(cluster):
    dictionary={}
    maxCount=0
    maxKind=''
    for point in cluster.points:
        category=point[1]
        if category not in dictionary:
            dictionary[category]=0
        dictionary[category]+=1
        if (dictionary[category]>maxCount):
            maxCount=dictionary[category]
            maxKind=category
    cluster.category=maxKind
    for category in dictionary:
        if category!=maxKind:
            cluster.countMisclasified+=dictionary[category]

def printOutput(clusters):
    fileToOpen=open(sys.argv[3],'w')
    mistake=0
    for key in clusters:
        cluster=clusters[key]
        fileToOpen.write('cluster:%s\n' % (cluster.category))
        for point in cluster.points:
            point[0].append(point[1])
            fileToOpen.write(str(point[0]))
            fileToOpen.write('\n')
        mistake+=cluster.countMisclasified
    fileToOpen.write('Number misclasified:%s' % (mistake))
    fileToOpen.close()

if __name__=='__main__':
    open_data = open(sys.argv[1]).readlines()
    K=int(sys.argv[2])
    data_list=[]
    for line in open_data:
        line=line.replace("\n","")
        sample=[]
        split=line.split(",")
        for i in range(len(split)-1):
            sample.append(float(split[i]))
        point=(sample,split[len(split)-1])
        data_list.append(point)
    clusters={}
    heap=[]
    for i in range(len(data_list)):
        clusters[i]=Cluster([data_list[i]],1,data_list[i][0])
    for i in range(len(clusters)-1):
        for j in range(i+1,len(clusters)):
            heap.append([cluster_distance(clusters[i],clusters[j]),i,j])
    heapq.heapify(heap)
    counter=len(clusters)
    while(len(clusters)>K):
        nearest=heapq.heappop(heap)
        id1=nearest[1]
        id2=nearest[2]
        if (id1 in clusters) and (id2 in clusters):
            newCluster=merge(clusters[id1],clusters[id2])
            counter+=1
            clusters.pop(id1)
            clusters.pop(id2)     
            for clusterId in clusters:
                array=[cluster_distance(newCluster,clusters[clusterId]),counter,clusterId]
                heapq.heappush(heap,array)
            clusters[counter]=newCluster
    category={}
    for clusterId in clusters:
        getKind(clusters[clusterId])
    printOutput(clusters)
        

