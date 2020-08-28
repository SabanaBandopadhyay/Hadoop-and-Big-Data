import sys
from pyspark import SparkContext
import operator
from operator import add
from math import sqrt


kMean=[]


def distanceSquared(a1,b1,a2,b2):
        return sqrt(((a2-a1)**2)+((b2-b1)**2))
    
    
def closestPoint(La,Lo,array):
    fl=float(La);flong=float(Lo);s=[]
    for i in array:
        j=i.split(",")
        
        La=float(j[0])
        Lo=float(j[1])
        s.append(sqrt(((fl-La)**2)+((flong-Lo)**2)))
    minIndex, minValue = min(enumerate(s), key=operator.itemgetter(1))
    kMean.append(minIndex)
    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "K-Mean Example - Device Status <file>"
        exit(-1)
    sparkc = SparkContext()
    
    
    deviceStatusData = sparkc.textFile(sys.argv[1], 1)
    deviceStatusData1 = deviceStatusData.map(lambda s: s.replace("/",","))
    deviceStatusData2 = deviceStatusData1.map(lambda s: s.replace("|",","))
    deviceStatusData3 = deviceStatusData2.map(lambda q:q.split(',')).map(lambda q : q[12]+","+q[13])
     
    deviceStatusData3.persist() 
    deviceStatusDataFinal=deviceStatusData3.collect()
   
    
    arrLat=[]
    arrLong=[]

    for i in deviceStatusDataFinal:
        if "0,0" not in i:
            j=i.split(",")
            arrLat.append(float(j[0]))
            arrLong.append(float(j[1]))

    arrayJoin=[]
    for lt,lg in zip(arrLat, arrLong):
        arrayJoin.append(str(lt)+","+str(lg))
        
    dt = sparkc.parallelize(arrayJoin)
    K=5
    convergeDistance=0.1
    
    kPoint = dt.takeSample(False, K, 42)
    
    dist=5
    while dist>convergeDistance:
        kMean=[]
        for i,j in zip(arrLat,arrLong):
            closestPoint(i,j,kPoint)
        addLat=[0,0,0,0,0];addLong=[0,0,0,0,0];c=[0,0,0,0,0]    

        for i,j,k in zip(kMean,arrLat,arrLong):
            addLat[i]=addLat[i]+j
            addLong[i]=addLong[i]+k
            c[i]=c[i]+1

        kPn=[]    
        
        for i,j,k in zip(addLat,addLong,c):
            n=i/k
            m=j/k
            kPn.append(str(n)+','+str(m))

        dist=0    

        for i,j in zip(kPoint,kPn):
            n=i.split(',')
            m=j.split(',')
            dist=dist+distanceSquared(float(n[0]),float(n[1]),float(m[0]),float(m[1]))  

        print(kPn)
        kPoint=kPn

    deviceStatusData3.unpersist() 
        
    sparkc.stop() 
    

