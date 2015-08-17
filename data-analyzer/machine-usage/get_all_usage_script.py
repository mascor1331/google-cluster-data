from src.processMachineUsage import *
import time 
#******************************************************#
# Data starts from May 1,2011, Sunday, 19:00 EDT, with
# interval of 300s. The initial time of data is 600s.
# (the first entrry of part-00000-of-00500.csv is 600s)
#******************************************************#
#=============get_hoursample_usage_script==============#

#Variable Initialization
timeSlotTotalNum=9000             			   # user-define value (here we set 8928 > 31*24*12 ) 
filenum=arange(0,500,1)          			   # filename that would be looped
initTime=600000000;    		     		       # start time of the third week=600000000 
startTime=datetime.datetime(2011,5,1,0,0)      # real time             
dataPath = '../../clusterdata-2011-2/'
usageDataPath = dataPath + 'task_usage/'       # data path of task_usage of clusterdata-2011-2
resultPath='all-data/'                         # path where results are saved
pathMachineId="../data-summary/machine_id.csv" # unique machineId.csv path, there are 12580 unique machine 
                                               # Ids in the entire 500 task_usage datasets
#Main Function 
t=time.time()	                              #compute the entire processing time
#invoke function from processMachineeUsage.py file with following parameters
processFile(usageDataPath,pathMachineId,filenum,initTime,timeSlotTotalNum,resultPath)
getTimeIdMatrix(timeSlotTotalNum,resultPath,startTime)
print ("the entire process takes %s seconds!"%(time.time()-t))

