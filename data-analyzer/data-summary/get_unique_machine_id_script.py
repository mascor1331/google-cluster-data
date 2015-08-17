from numpy import *
import csv
import gzip
import datetime

#Variable Initialization
dataPath ='../../clusterdata-2011-2/'
usageDataPath =dataPath + 'task_usage/'
pathMachineId="../data-summary/machine_id.csv"

def getMachineIdMatrix(usageDataPath,pathMachineId):
	filenum=arange(1,500,1)         #process all data, not user defined variables
	timeSlot=0 
	path_ini=(usageDataPath+'part-00000-of-00500.csv.gz')
	print ("Loading %s"%(path_ini)),"\n"
	mac = genfromtxt(path_ini, delimiter=',',usecols=4)
	uniqueId=set(mac)
	for i in filenum:
		path=(usageDataPath+"part-%05d-of-00500.csv.gz"%(i))
		print ("Loading %s"%(path)),"\n"
		macId=genfromtxt(path, delimiter=',',usecols=4)
		uniqueIdNext=set(macId)
		uniqueId=uniqueId.union(uniqueIdNext)			
		
	uniqueMacId=set(uniqueId)
	tolistId=list(uniqueMacId)
 	numOfMachine=len(tolistId)
 	Id=unique(tolistId)
 	numRange=arange(0,numOfMachine,1)
 	result=vstack((numRange,Id)).T
	print ('there are altogether %s different machines!'%(numOfMachine))
	savetxt(pathMachineId,result,delimiter=',')

#main function
getMachineIdMatrix(usageDataPath, pathMachineId)