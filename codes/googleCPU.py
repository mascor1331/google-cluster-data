from numpy import *
import time; 
#path="D:\\GoogleData\\part-00200-of-00500.csv"

def getUniqueMacId():
	filenum=arange(1,500,1)
	path_ini="/home/jmzhu/googleData/task_usage/part-00000-of-00500.csv"
	print ("Loading %s"%(path_ini)),"\n"
	mac = genfromtxt(path_ini, delimiter=',',usecols=4)
	uniqueId=set(mac)
	for i in filenum:
		path=("/home/jmzhu/googleData/task_usage/part-%05d-of-00500.csv"%(i))
		print ("Loading %s"%(path)),"\n"
		macId=genfromtxt(path, delimiter=',',usecols=4)
		uniqueIdNext=set(macId)
		print uniqueIdNext.shape
		print type(uniqueIdNext)
		uniqueId=hstack((uniqueId,uniqueIdNext))	
	savetxt('UniquemachineId_composite.csv',uniqueId,delimiter=',')			
	
	uniqueMacId=set(uniqueId)
 	numOfMachine=len(uniqueMacId)
	print ('there are altogether %s different machines!'%(numOfMachine))
	savetxt('UniquemachineId.csv',uniqueMacId,delimiter=',')
	
def processHourSamples():
	path_macId="E:\\Bigdata\\googleCluster\\data\\UniquemachineId.csv"
	print ("Loading all the machineIds...")
	uniqueId=genfromtxt(path_macId,delimiter=",")
	numOfMachine=len(uniqueId)
	print ('there are altogether %s different machines!'%(numOfMachine))
     
	filenum=arange(201,203,1)
	path_ini="D:\\GoogleData\\part-00200-of-00500.csv.gz"
	initTime=1002900000000;
	totalTimeSlot=0
	print ("Loading %s ..."%(path_ini)),"\n"
	dataSet_ini = genfromtxt(path_ini, delimiter=',',usecols=(0,1,4,5,6))
	newDataset_ini,timeSlotNumPerfile_ini,initTime=timeSlotProcess(dataSet_ini, initTime)
	timeMacCPUMatrix=sumOfCPU(newDataset_ini,numOfMachine,timeSlotNumPerfile_ini,uniqueId)
	totalTimeSlot+=timeSlotNumPerfile_ini

	for i in filenum:
		path=("D:\\GoogleData\\part-%05d-of-00500.csv.gz"%(i))
		print ("Loading %s ..."%(path)),"\n"
		dataSet = genfromtxt(path, delimiter=',',usecols=(0,1,4,5,6))
		newDataset,timeSlotPerFile,initTime=timeSlotProcess(dataSet,initTime)
		newTimeMacCPUMatrix=sumOfCPU(newDataset,numOfMachine,timeSlotPerFile,uniqueId)
		totalTimeSlot+=timeSlotPerFile
		print "total timeslot number is ---> ", totalTimeSlot
		if totalTimeSlot > 7*24*12:
			break
		timeMacCPUMatrix=hstack((timeMacCPUMatrix,newTimeMacCPUMatrix))
	savetxt('sumOfTimeMachineCPU_week.csv',timeMacCPUMatrix,delimiter=',')

	
def timeSlotProcess(timeMacMatrix,initTime):
	print "Divide into timeslot..."
	numOfLines=timeMacMatrix.shape[0]
	slotAndTimeDiff=zeros((numOfLines,2))
	timeSlot=1
	for line,row in enumerate(timeMacMatrix):
		if (row[0]>=initTime and row[1]<= initTime+300000000):
			slotAndTimeDiff[line,0]=timeSlot
		elif row[0]< initTime:
			continue
		else:
			timeSlot+=1
			print "current file has time slot number == ",timeSlot,'\n'
			initTime+=300000000
			slotAndTimeDiff[line,0]=timeSlot
	slotAndTimeDiff[:,1]=((timeMacMatrix[:,1]-timeMacMatrix[:,0])/300000000)*timeMacMatrix[:,3]
	print ("altogether there are abouot %d timeslot in this file"%(timeSlot))
	timeMacMatrix=hstack((slotAndTimeDiff,timeMacMatrix))
	timeMacMatrix=delete(timeMacMatrix,[2,3],1)
	return timeMacMatrix,timeSlot,initTime
#returned timeMacMatrix   column:   0                1                  2            3       4   
#                         valules: timeslot   (timediff/3seconds)*cpu   machineId   CPU   memory

		
def sumOfCPU(timeMacMatrix,numOfMachine,timeSlotNumPerfile,uniqueMacId):
	print "computing the sum of CPU rate..."
	timeMacCPUMatrix=-1*ones((numOfMachine,timeSlotNumPerfile))
	for line,row in enumerate(timeMacMatrix):
		indexs=where(uniqueMacId==row[2])
		if timeMacCPUMatrix[indexs,row[0]-1]==-1:
			timeMacCPUMatrix[indexs,row[0]-1]=0
		else:
			timeMacCPUMatrix[indexs,row[0]-1]+=row[1]
	return timeMacCPUMatrix 		

def processOneFile(path):
	print "File loading..."
	dataSet = genfromtxt(path, delimiter=',',usecols=(0,1,4,5,6))
	numOfLines,Columns=shape(dataSet)
	print ("File load successfully with %d lines %d columns" %(numOfLines,Columns))	
	#dataSet has the following format:
	#column  0           1           2            3       4    
	#       startTime   endTime    machineId     cpu     memory	
	#timeMac_CPU_Memo=zeros((numOfLines,5))
	machineId=zeros((numOfLines,1))
	machineId=dataSet[:,2]
	uniqueMacIdset=set(machineId)
	uniqueMacId=list(uniqueMacIdset)
	numOfMachine=len(uniqueMacId)
	print ('there are altogether %s different machines!'%(numOfMachine))
	savetxt('UniquemachineIdPart200.csv',uniqueMacId,delimiter=',')
	newDataSet,timeSlotNum,initTime=timeSlotProcess(dataSet,1002900000000)#dataSet[0][0])
	#newDataSet format is :
	#column    0                          1              2            3
	#        timeslot(start from 1)    machineId        CPU         memory	
#	savetxt('timeMachineCPU_Memory.csv',newDataSet,delimiter=',')
#	print "saving timeMachineCPU_Memory.csv file time = ", time.time()-t1
	t1 = time.time()
	timeMacCPUMatrix=sumOfCPU(newDataSet,numOfMachine,timeSlotNum,uniqueMacId)
	print ("it takes %s seconds to compute the sum"%(time.time()-t))
	savetxt('sumOfTimeMachineCPU.csv',timeMacCPUMatrix,delimiter=',')	

t=time.time()	
processOneFile("E:\Bigdata\googleCluster\data\part-00200-of-00500.csv")
print ("the entire process takes %s seconds!"%(time.time()-t))
