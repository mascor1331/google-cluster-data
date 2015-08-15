from numpy import *
import time 
import csv
import gzip
import datetime
#******************************************************#
# Data starts from May 1,2011, Sunday, 19:00 EDT, with
# interval of 300s. The initial time of data is 600s.
# (the first entrry of part-00000-of-00500.csv is 600s)
# We preprocess one entire week data from 00:00 EDT, May 16,
# Monday of the third week. the start time is 1228200s(600+7*24*12*300*2+5*12*300)
# which starts at part-00244-of-00500.csv
#******************************************************#

def getUniqueMacId():
	filenum=arange(245,370,1)
	timeSlot=0 
	path_ini=("../clusterdata-2011-2/task_usage/part-00244-of-00500.csv")
	print ("Loading %s"%(path_ini)),"\n"
	mac = genfromtxt(path_ini, delimiter=',',usecols=4)
	uniqueId=set(mac)
	print len(list(uniqueId))
	for i in filenum:
		path=("../clusterdata-2011-2/task_usage/part-%05d-of-00500.csv"%(i))
		print ("Loading %s"%(path)),"\n"
		macId=genfromtxt(path, delimiter=',',usecols=4)
		uniqueIdNext=set(macId)
		uniqueId=uniqueId.union(uniqueIdNext)			

	uniqueMacId=set(uniqueId)
	tolistId=list(uniqueMacId)
 	numOfMachine=len(tolistId)
 	Id=unique(tolistId)
	print ('there are altogether %s different machines!'%(numOfMachine))
	savetxt('UniquemachineId_thirdWeek.csv',Id,delimiter=',')

def getTimeIdMatrix():
	timeSlotTotalNum=7*24*12
	t=datetime.datetime(2011,5,16,0,0)
	delta=datetime.timedelta(minutes=5)
	realTime_id=zeros((timeSlotTotalNum,2),dtype='|S20')
	for timeSlot in range(timeSlotTotalNum):
		realTime_id[timeSlot,0]=str(timeSlot)
		strtime=t.strftime("%Y-%m-%d %H:%M:%S")
		realTime_id[timeSlot,1]=strtime
		t=t+delta
	savetxt('realTimeIdMatrix_thirdWeek.csv',realTime_id,delimiter=',',fmt='%s')
	
def processWeekFiles():
	#Variable Initialization
	timeSlotTotalNum=7*24*12        			   # user-define value (here represents one week) 
	filenum=arange(244,400,1)          			   # filename that would be looped
	initTime=1228200000000; 		     		   # start time of the third week
	timeSlot=0                                     # increase as the for loop continue
	flag=2                                         # initial value of flag, used to break multiple loops	
	t=datetime.datetime(2011,5,16,0,0)             # start time of this trace	
	delta=datetime.timedelta(minutes=5)			   # time interval
	realTime_id=zeros((timeSlotTotalNum,2),dtype='|S20')
	path_macId="../UniquemachineId_thirdWeek.csv"  # server path of unique machineId 
	#path_macId="..\\data\\generated_data\\UniquemachineId_thirdWeek.csv"  #local path of uniquemachineId
	
	# Get total number of machines
	print ("Loading all the machineIds...")
	uniqueId=genfromtxt(path_macId,delimiter=",")
	numOfMachine=len(uniqueId)
	idIndexDict = { v:k for k,v in enumerate(uniqueId) }
	print ('there are altogether %s different machines!'%(numOfMachine))
	
	
	#initialize matrixes of numofMachine * timeSlotNum with all entry value -1
	timeMacCPUMatrix=-1*ones((numOfMachine,timeSlotTotalNum))  #matrix that used to store cpu
	timeMacMemoMatrix=-1*ones((numOfMachine,timeSlotTotalNum)) #matrix that used to store memory
	#loop certain number files 
	for i in filenum:		
		path=("../clusterdata-2011-2/task_usage/part-%05d-of-00500.csv"%(i))     #server path of task_usage data 
		#path=("D:\\GoogleData\\part-%05d-of-00500.csv"%(i))                     #local path of task_usage data
		print ("Loading %s ..."%(path)),"\n"
		with open(path,'rb') as f:
			reader = csv.reader(f,delimiter=",")
			for row in reader:
				endTime=int(row[1])           #convert row elements of string type into corresponding data type
				startTime=int(row[0])         #i.e. startTime, endTime, machineId, cpu, memory
				machineId=int(row[4])
				cpu=float(row[5])
				memory=float(row[6])
				timeDiff=endTime-startTime
				indexs=idIndexDict[machineId] #get the corresponding index of specific machineId
				if(startTime>=initTime and endTime <= initTime+300000000):
					#realTime_id[timeSlot,0]=str(timeSlot)
					#strtime=t.strftime("%Y-%m-%d %H:%M:%S")
					#realTime_id[timeSlot,1]=strtime
					if timeMacCPUMatrix[indexs,timeSlot]==-1:
						timeMacCPUMatrix[indexs,timeSlot]=0
					else:
						timeMacCPUMatrix[indexs,timeSlot]+=(timeDiff/300000000.0)*cpu
					
					if timeMacMemoMatrix[indexs,timeSlot]== -1:
						timeMacMemoMatrix[indexs,timeSlot]=0
					else:
						timeMacMemoMatrix[indexs,timeSlot]+=(timeDiff/300000000.0)*memory
				elif startTime<initTime:
					continue
				else:
					timeSlot+=1
					#t=t+delta
					if timeSlot>timeSlotTotalNum-1:
						flag=1               #used to break out of multiple for loops
						break
					print "current time slot is ",timeSlot,'\n'
					initTime+=300000000
					if timeMacCPUMatrix[indexs,timeSlot]==-1:
						timeMacCPUMatrix[indexs,timeSlot]=0
					else:
						timeMacCPUMatrix[indexs,timeSlot]+=(timeDiff/300000000.0)*cpu
					
					if timeMacMemoMatrix[indexs,timeSlot]== -1:
						timeMacMemoMatrix[indexs,timeSlot]=0
					else:
						timeMacMemoMatrix[indexs,timeSlot]+=(timeDiff/300000000.0)*memory
		if flag==1:
			break                            #used to break out of multiple for loops
	savetxt('sumOfTimeMachineCPU_thirdWeek.csv',timeMacCPUMatrix,delimiter=',') 
	savetxt('sumOfTimeMachineMemo_thirdWeek.csv',timeMacMemoMatrix,delimiter=',') 
	#savetxt('realTimeIdMatrix_thirdWeek.csv',realTime_id,delimiter=',',fmt='%s')

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
	
	#newDataSet format is :
	#column    0                          1              2            3
	#        timeslot(start from 1)    machineId        CPU         memory	
#	savetxt('timeMachineCPU_Memory.csv',newDataSet,delimiter=',')
#	print "saving timeMachineCPU_Memory.csv file time = ", time.time()-t1
 	idIndexDict = { v:k for k,v in enumerate(uniqueMacId) }
	for line,row in enumerate(dataSet):
		    timeDiff=row[1]-row[0]
		    indexs=idIndexDict[row[2]]
		    if(row[0]>=initTime and row[1] <= initTime+300000000):
		    	if timeMacCPUMatrix[indexs,timeSlot]==-1:
		    		timeMacCPUMatrix[indexs,timeSlot]=0
		    	else:
		    		timeMacCPUMatrix[indexs,timeSlot]+=(timeDiff/300000000)*row[3]
		    elif row[0]<initTime:
		    	continue
		    else:
		    	timeSlot+=1
		    	print "current time slot is ",timeSlot,'\n'
		    	initTime+=300000000
		    	if timeMacCPUMatrix[indexs,timeSlot]==-1:
		    		timeMacCPUMatrix[indexs,timeSlot]=0
		    	else:
		    		timeMacCPUMatrix[indexs,timeSlot]+=(timeDiff/300000000)*row[3]
	savetxt('sumOfTimeMachineCPU_week.csv',timeMacCPUMatrix,delimiter=',')

#Main 
t=time.time()	
processWeekFiles()
print ("the entire process takes %s seconds!"%(time.time()-t))
