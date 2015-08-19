from numpy import *
import time 
import csv
import gzip
import datetime
#============================================================================#
# Function processFile is used to preprocess data between user-defined time,
# it returns two files of task_usage CPU and Memory data

# Input:
# usageDataPath: the directory of task_usage data, there should be 500 "*.csv.gz"
#                 files in this directory
# pathMachineId: the path of unique machine_id.csv file. Unique machineId 
#                is preprocessed according to a function getMachineIdMatrix,
#                which returns a csv file with 12580 machineIds in all data.
# filenum: the corresponding file number range of the time interval that we
#          want to process. 
# initTime: the start time of preprocessed tracing data, it should be given
#           by user.
# timeSlotNum: the number of tiny time interval, with 5 minutes as time interval
#              for example, we want to process 1 hour data, then timeSlotNum is 
#              60 mins/(5 mins/interval)=12 intervals
# resultPath: the path of resulted file that supposed to be saved

# Output: two csv files are saved after this process, one is used to store cpu 
#         rate, another is memory rate
#============================================================================#


def processFile(usageDataPath,pathMachineId,filenum,initTime,timeSlotTotalNum,resultPath):
	#variables that not user defined
	timeSlot=0                                      # increase as the for loop continue
	timeInterval=300000000.0                        # data trace ever five minutes(300s)
	flag=2                                          # initial value of flag, used to break multiple loops	
	
	delta=datetime.timedelta(minutes=5)			    # time interval
	# time_id=zeros((timeSlotTotalNum,2),dtype='|S20')# time_id used to save time id pair
    
	#process starts
	# Get total number of machines
	print ("Loading all the machineIds...")
	uniqueId=genfromtxt(pathMachineId,delimiter=",")
	numOfMachine=len(uniqueId)
	idIndexDict = { v[1]:v[0] for v in uniqueId }
	print ('there are altogether %s different machines!'%(numOfMachine))	
	
	#initialize matrixes of numofMachine * timeSlotNum with all entry value -1
	timeMacCPUMatrix=-1*ones((numOfMachine,timeSlotTotalNum))  #matrix that used to store cpu
	timeMacMemoMatrix=-1*ones((numOfMachine,timeSlotTotalNum)) #matrix that used to store memory
	
	#loop certain number files 
	for i in filenum:		
		path=(usageDataPath+"part-%05d-of-00500.csv.gz"%(i))     # path of task_usage data 
		print ("Loading %s ..."%(path)),"\n"
		with gzip.open(path,'rb') as f:
			reader = csv.reader(f,delimiter=",")
			for row in reader:
				endTime=int(row[1])           # convert row elements of string type into corresponding data type
				startTime=int(row[0])         # i.e. startTime, endTime, machineId, cpu, memory
				machineId=int(row[4])
				cpu=float(row[5])
				memory=float(row[6])
				timeDiff=endTime-startTime
				indexs=idIndexDict[machineId] #get the corresponding index of specific machineId
				if(startTime>=initTime and endTime <= initTime+timeInterval):
					# time_id[timeSlot,0]=str(timeSlot+1)
					# strtime=t.strftime("%Y-%m-%d %H:%M:%S")
					# time_id[timeSlot,1]=strtime
					if timeMacCPUMatrix[indexs,timeSlot]==-1:
						timeMacCPUMatrix[indexs,timeSlot]=0
					else:
						timeMacCPUMatrix[indexs,timeSlot]+=(timeDiff/timeInterval)*cpu
					
					if timeMacMemoMatrix[indexs,timeSlot]== -1:
						timeMacMemoMatrix[indexs,timeSlot]=0
					else:
						timeMacMemoMatrix[indexs,timeSlot]+=(timeDiff/timeInterval)*memory
				elif startTime<initTime:
					continue
				else:
					timeSlot+=1
					# t=t+delta
					if timeSlot>timeSlotTotalNum-1:
						flag=1               #used to break out of multiple for loops
						break
					print "current time slot is ",timeSlot,'\n'
					initTime+=timeInterval
					if timeMacCPUMatrix[indexs,timeSlot]==-1:
						timeMacCPUMatrix[indexs,timeSlot]=0
					else:
						timeMacCPUMatrix[indexs,timeSlot]+=(timeDiff/timeInterval)*cpu
					
					if timeMacMemoMatrix[indexs,timeSlot]== -1:
						timeMacMemoMatrix[indexs,timeSlot]=0
					else:
						timeMacMemoMatrix[indexs,timeSlot]+=(timeDiff/timeInterval)*memory
		if flag==1:
			break                            #used to break out of multiple for loops
	savetxt(resultPath+'machine_cpu_usage_matrix_sample.csv',timeMacCPUMatrix,delimiter=',') 
	savetxt(resultPath+'machine_memory_usage_matrix_sample.csv',timeMacMemoMatrix,delimiter=',')
	#savetxt(resultPath+'time_id.csv',time_id,delimiter=',',fmt='%s')


# get time_id of the processed time, this function returns a .csv file named time_id.csv
def getTimeIdMatrix(timeSlotTotalNum,resultPath,startTime):
	delta=datetime.timedelta(minutes=5)
	realTime_id=zeros((timeSlotTotalNum,2),dtype='|S20')
	for timeSlot in range(timeSlotTotalNum):
		realTime_id[timeSlot,0]=str(timeSlot+1)
		strtime=startTime.strftime("%Y-%m-%d %H:%M:%S")
		realTime_id[timeSlot,1]=strtime
		startTime=startTime+delta
	savetxt(resultPath+'time_id.csv',realTime_id,delimiter=',',fmt='%s')


