import os
import json
import shutil

cacheGeneralCounter = 0 # used for LRU
cache=[{},{},{}] # cache with place for 3 pages
logBuffer=[] # log buffer
line=0 # used for telling us what is the line we are reading
jumpToNextLine=0 # used in order to skip all stoping until this line
activetrans=[] # list of a tuple of active transaction and there last sequence 
sequence = 1 # sequence counter
fileName=''
forceDeleteLogBuffer=True
MasterRecord={} # StartPointer, LastCP


#create a empty Stable Storage If needed delete old one
def createStableStorage():
    folder = './stablestorage'
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)
    for i in range(1,21):
        if (os.path.isfile('./stablestorage/'+ str(i)) == False):
            page = {}
            page["id"]=i
            page["psn"]=0
            page["content"]=[" "]*20
            json.dump(page, open('./stablestorage/'+ str(i),"w+"))
    if(os.path.isfile('./stablestorage/stablelog') == False):
        open('./stablestorage/stablelog',"ab")

#param: log sequence
#return: the index of the log with sequnce on log buffer
def findIndex_SeqNuM(seq):
    counter=0
    for log in logBuffer:
        if(log["lsn"]==seq):
            return counter;
        counter+=1
    return -1;


#abort tid
def abort(tid):
    global sequence
    global MasterRecord
    global activetrans
    global logBuffer
    global forceDeleteLogBuffer
    forceDeleteLogBuffer=False
    stab=readLogStable()
    logBuffer=stab+logBuffer
    LastSeqNo=findIndex_SeqNuM(checkLSNofTrans(tid))
    while(LastSeqNo!=-1):
        printt("LastSeqNo="+str(LastSeqNo))
        if(logBuffer[LastSeqNo]["actiontype"] == "write"):
            inverse(logBuffer[LastSeqNo])
        LastSeqNo=logBuffer[LastSeqNo]["PreviousSN"]
    logentry={"lsn": sequence, "actiontype":"rollback", "tid": tid,"PreviousSN":checkLSNofTrans(tid)}
    logBuffer.append(logentry)
    printt("trans=" + str(tid)  + "is aborted. \n" +
          "creating new log entry with " + str(logentry))
    activetrans.remove({"tid": tid, "lsn": checkLSNofTrans(tid)})
    printt("trans=" + str(tid)  + " is now not in Activators \n")
    for tran in activetrans:
       if(tran["tid"]==tid):
           tran["lsn"]=sequence
    forceDeleteLogBuffer=True
    force()
    sequence+=1
    printt("abort finished")



# checks if a page is in cache and return where in cache, if not in cache return -1
def checkPageInCache(id):
    printt("checkig if page on cache")
    for i in range (0,3):
        if(cache[i]!={} and cache[i]["page"]["id"] == id):
            print("page found on i=" + str(i))
            return i
    printt("page not in cache")
    return -1

#param: page id
#return: the location of the page on cache (but it brings from cache)
def fetch(id):
    printt("start fetching page=" + str(id))
    global cacheGeneralCounter
    global cache
    lru=LRU()
    if(cache[lru] != {}):
        printt("the cache is full writing lru=" + str(lru))
        flushByCacheLocation(lru)
    cache[lru]["page"]=json.load(open('./stablestorage/'+ str(id)))
    cache[lru]["page"]["status"] = 0
    cache[lru]["cacheCounter"] = cacheGeneralCounter
    cacheGeneralCounter += 1
    return lru

#param: pageid
#return: true if there is a log rellavant to that pageid in log buffer
def checkForLogEntry(pageid):
    print("!!! pageid=" + str(pageid))
    for log in logBuffer:
        try: 
            x=log["page"]
            print("!!!page="+str(x))
            if(x==pageid):
                return True
        except:
            pass
    return False

#param: cache index location
#return: none, flushs the page on the param 
def flushByCacheLocation(i):
    global cache
    printt("checking if the page has a relevant log entry")
    if(checkForLogEntry(cache[i]["page"]["id"])==True):
        printt("there is a log with relavnt pageid0, forcing")
        force()
    printt("writing page to stable storage")
    page={}
    page["id"] = cache[i]["page"]["id"]
    page["psn"] = cache[i]["page"]["psn"]
    page["content"]=cache[i]["page"]["content"]
    print(str(cache[i]["page"]))
    print(str(page))
    cache[i]["page"]["status"] == "clean"
    
    json.dump(page, open('./stablestorage/'+ str(cache[i]["page"]["id"]),"w+"))
    printt("writing was completed")

#Checkpoint
def checkpoint():
    global sequence
    global MasterRecord
    global activetrans
    tid=[]
    for i in range (0,3):
        if(cache[i]!={} and  cache[i]["page"]["status"]=='dirty'):
             flush(cache[i]["page"]["id"])
             tid.append(int(cache[i]["page"]["id"]))
    logentry = {"lsn": sequence, "actiontype":"checkpoint", "tid": tid, "PreviousSN":None}
    printt("trans=" + str(tid)  + "are flushed because of checkpoint action. \n" +
          "creating new log entry with " + str(logentry))
    logBuffer.append(logentry)
    force()
    MasterRecord["LastCP"]=sequence
    sequence+=1
    printt("checkpoint finished")

#param: page id
#return none, but flush a page with param to disk
def flush(pageid):
    for i in range(0,3):
        if(cache[i]["page"]["id"]==pageid):
            flushByCacheLocation(i)
            return
    printt("The cache has no page="+str(pageid))
    return

#force
def force():
    global logBuffer
    stab=readLogStable()
    sequncesInStableLog=[]
    printt("start forcing, writing logBuffer into stable log")
    for log in stab:
        sequncesInStableLog.append(int(log["lsn"])) 
    for log in logBuffer:
        if(int(log["lsn"]) in sequncesInStableLog):
            continue
        printt("writing log entry with params " + str(log))
        json.dump(log, open('./stablestorage/stablelog',"a+"))
    printt("finished writing logBuffer to stable log")
    if(forceDeleteLogBuffer=True):
        logBuffer=[]
    printt("force completed")

#param: transaction id
#return: none, but commit the tranaction with param
def commit(tid):
    global sequence
    global activetrans
    global logBuffer
    printt("Remove trans= " + str(tid) + " from activetrans")
    activetrans.remove({"tid": tid, "lsn": checkLSNofTrans(tid)})
    logentry={"lsn": sequence, "actiontype":"commit", "tid": tid,"PreviousSN":None}
    printt("trans=" + str(tid)  + " is now not in Activators \n" + "creating new log entry with " + str(logentry))
    logBuffer.append(logentry)
    for tran in activetrans:
       if(tran["tid"]==tid):
           tran["lsn"]=sequence
    force()
    sequence+=1
    printt("commit finished")


#param transaction id
#return none, but creates begin log
def begin(tid):
    global sequence
    printt("adding trans=" + str(tid) + "activetrans")
    if (activetrans=={}):
        MasterRecord[StartPointer]=sequence
    activetrans.append({"tid": tid, "lsn": sequence})
    logentry={"lsn": sequence, "actiontype":"begin", "tid": tid,"PreviousSN":None}
    

    printt("trans=" + str(tid)  + " is now in activeTrans\n" + 
        "creating new log entry with " + str(logentry))
    logBuffer.append(logentry)
    sequence+=1
    printt("begin finished")

    

#return the page of least recently used 
def LRU():
    for i in range(0,3):
        if(cache[i]=={}):
            printt("empty cache found on cache page i=" +str(i))
            return i
    minimum = min(cache[0]["cacheCounter"],cache[1]["cacheCounter"],cache[2]["cacheCounter"])
    for i in range(0,3):
        if(cache[i]["cacheCounter"]==minimum):
            printt("least used page is in cache pag i="+ str(i))
            return i
    
#param transaction id, page id, length of value, offset from where to start, value string to put
# return none, but writing to cache page with  page id the new value starting from offset and new log entry 

def write(tid,id,length,offset,value,flag=1):
    print("write "+ str(value))
    global cache
    global cacheGeneralCounter
    global activetrans
    global sequence
    global logBuffer
    printt("checking if the trans=" + str(tid) +  " is in active trans")
    if(checkLSNofTrans(tid)==-1):
        printt("trans with tid=" + str(tid) + "not in activetrans, adding to active trans")
        begin(tid)
    printt(str(tid) + " is on activetrans")
    printt("checking if page on cache")
    cached=checkPageInCache(id)
    if(cached == -1):
        printt("page not on cache")
        cached=fetch(id)
        printt("page is now on cache at i=" + str(cached))
    else:
        printt("page on cache at i=" + str(cached))
    printt("change content and psn")
    old_value=''
    print("offset is: " + str(offset) + " offset + length is: " + str(length))
    for i in range(offset,offset+length):
        print("value is: " + value)
        print("i is: " +str(i) + "i-offset is:" + str(i-offset))
        old_value=old_value+str(cache[cached]["page"]["content"][i])
        print("value char: "+ str(value[i-offset]))
        cache[cached]["page"]["content"][i]=value[i-offset]
        cacheGeneralCounter+=1
        cache[cached]["cacheCounter"]=cacheGeneralCounter
    lastpsn=cache[cached]["page"]["psn"]
    cache[cached]["page"]["psn"]=sequence   
    cache[cached]["page"]["status"]="dirty"
    logentry={"page": id, "lsn": sequence, "actiontype":"write", "tid": tid,"PreviousSN":checkLSNofTrans(tid),"length":length, "offset":offset,"value":value,"oldValue":old_value}
    
    for tran in activetrans:
        if(tran["tid"]==tid):
            tran["lsn"]=sequence
    logBuffer.append(logentry)
    sequence+=1
    printt("write finished")

#get trans id and return the last sequence number of it, -1 if there is no tid)
def checkLSNofTrans(tid):
    for tran in activetrans:
        if(tran["tid"]==tid):
            return tran["lsn"]
    return -1

#param tranaction id
#return the last sequence elavnt to the tranaction 
def checkLSNofTrans(tid):
    for tran in activetrans:
        if(tran["tid"]==tid):
            return tran["lsn"]
    return -1

#function used for printing massages and showing variable while runing
def printt(string=None):
    global jumpToNextLine
    if(string != None):
        print(string)
    if(jumpToNextLine > line):
        return
    read=input()
    parsed=read.split(" ")
    
    if(parsed[0] == "n"): 
        return
    #elif(parsed[0] == "M"): 
        #printCachePage(int(parsed[1]))
        #printt()
    elif(parsed[0]=="c"):
        jumpToNextLine = line+1
    elif(parsed[0]=="J"):
        jumpToNextLine = int(parsed[1])
    elif(parsed[0]=="l"):
        print(logBuffer)
        printt()
    elif(parsed[0]=="m"):
        printCachePage(int(parsed[1]))
        printt()
    elif(parsed[0]=="M"):
        printStablePage(int(parsed[1]))
        printt()
    elif(parsed[0]=="a"):
        printt(str(activetrans))
    elif(parsed[0]=="b"):
        printt(str(cache))
    elif(parsed[0]=="Z"):
        printCommands()
        printt()
    elif(parsed[0]=="L"):
        printt(readLogStable())
    elif(parsed[0]=="t"):
        for i in range(0,3):
            if(cache[i] !={} ):
               if(cache[i]["page"]["status"] == 'dirty'):
                    print(cache[i]["page"])
    elif(parsed[0]=="K"):
         quit()
    elif(parsed[0]=="r"):
           for tran in activetrans :
                print(tran)
    elif(parsed[0]=="s"):
        printt(sequence)

          

#param: page id
# reutrn none, but prints the page as in disk        
def printStablePage(pageid):
    print(json.load(open('./stablestorage/'+ str(pageid))))

#param: page id
# reutrn none, but prints the page as in cache or saying it is not on cache  
def printCachePage(pageid):
    for i in range(0,3):
        if(cache[i]!={}):
            if(int(cache[i]["page"]["id"])==pageid):
                print(cache[i]["page"])
                return
    printt("there is no page on cache, with id="+str(pageid))
    return
    
def createLogEntry():
    pass

#prints the command left on undone on our commands file
def printCommands():
    global line
    f=open(fileName)
    lines=f.readlines()
    for i in range(line,len(lines)):
        print(lines[i])

#reads the commands file and parse it
def readFile(filepath):
    global line
    global fileName
    fileName = filepath
    with open(filepath) as fp:  
        row = fp.readline()
        while row:
            if "UPDATE" in row:
                parsed=row.split(":")
                tid=parsed[0]
                parsed1=parsed[1].split("UPDATE")
                parsed2=parsed1[1].split(",")
                print(("readfile: " + str(parsed2[3][1:-1])))
                write(int(tid), int(parsed2[0]), int(parsed2[1]), int(parsed2[2]), str(parsed2[3][2:-2]))
            elif "COMMIT" in row:
                commit(int(row.split(":")[0]))
            elif "ABORT" in row:
                abort(int(row.split(":")[0]))
            elif "CHECKPOINT":
                checkpoint()
            row = fp.readline()
            line += 1
            printt()

#reads the stable from disk to cache (log buffer)
def readLogStable():
    stablelog=[]
    with open("./stablestorage/stablelog") as fp:
        row = fp.readline()
    parsed=row.split("}")
    for p in parsed[:-1]:
        temp=json.loads(str(p+"}"))
        stablelog.append(temp)
    return stablelog


#redo algo
def redo():
    global cacheGeneralCounter 
    global cache
    global logBuffer
    global sequence 
    printt("redo phase start reading stablelog")
    logBuffer=readLogStable()
    printt("stablelog is now at log buffer")

    for log in logBuffer:
        printt("checking if log type is write or compensaton, log type is: " + str(log["actiontype"]) )
        if(log["actiontype"]=="write" or log["actiontype"]=="compensation"):
            printt("log type is "+str(log["actiontype"]) + "  , fetching page")    
            if(checkPageInCache((int(log["page"]))) == -1):
                print(checkPageInCache((int(log["page"]))))
                cached=fetch(int(log["page"]))
            else:
                cached=checkPageInCache((int(log["page"])))
            printt("page is now on cache page=" +str(cached) + " checking if page sequence is smaller then log sequence")
            if(cache[cached]["page"]["psn"]<log["lsn"]):
                printt("page sequence is smaller, redo log: checking if compansation or write")
                for i in range(int(log["offset"]),int(log["offset"])+int(log["length"])):
                    cache[cached]["page"]["content"][i]=log["value"][i-log["offset"]]
                    cacheGeneralCounter+=1
                    cache[cached]["cacheCounter"]=cacheGeneralCounter
                cache[cached]["page"]["psn"]=log["lsn"]
                cache[cached]["page"]["status"]="dirty"
        sequence = log["lsn"]+1
    print(str(cache))
    

#param: logentry
# return none, but create and do reverse to the recived log entry 
def inverse(logentry):
    printt("starting inverse of log")
    global cache
    global cacheGeneralCounter
    global activetrans
    global sequence
    global logBuffer
    cached=checkPageInCache(logentry["page"])
    if(cached == -1):
        printt("page not on cache")
        cached=fetch(logentry["page"])
        printt("page is now on cache at i=" + str(cached))
    else:
        printt("page on cache at i=" + str(cached))
    printt("change content and psn")
    for i in range(logentry["offset"],logentry["offset"]+logentry["length"]):
        print(str(logentry["oldValue"]))
        print(str(logentry["oldValue"][i-logentry["offset"]]))
        cache[cached]["page"]["content"][i]=logentry["oldValue"][i-logentry["offset"]]
        cacheGeneralCounter+=1
        cache[cached]["cacheCounter"]=cacheGeneralCounter
    lastpsn=cache[cached]["page"]["psn"]
    cache[cached]["page"]["psn"]=sequence   
    cache[cached]["page"]["status"]="dirty"
    logentry={"lsn": sequence, "actiontype":"compensation","NextUndoSeqNo":logentry["PreviousSN"],"page":logentry["page"],"offset":logentry["offset"],"value":logentry["oldValue"],"length":logentry["length"]}
    logBuffer.append(logentry)
    sequence+=1
    printt("compansisiton finished" + str(logentry))
    printt(str(cache))

#undo
def undo():
    printt('undo begin!')
    global sequence
    global MasterRecord
    global activetrans
    global logBuffer

    logBuffer= readLogStable()
    loser=[];
    printt("build losers and max lsn data type")
    for log in logBuffer:
        if(log["actiontype"] == "begin"):
            loser.append([int(log["tid"]),-1])
        if(log["actiontype"] == "commit" or log["actiontype"] == "rollback"):
           for los in loser:
               if(los[0]==int(log["tid"])):
                   loser.remove(los)
                   break
    for los in loser:
        for log in logBuffer:
            if(int(log["tid"] == los[0] )):
                los[1] = int(log["lsn"])
        
    printt(str(loser))

    while(len(loser) != 0):
            LastSeqNo_tid, LastSeqNo = findMaxSeq_InLosers(loser)
            printt("the max seq is: " + str(LastSeqNo)+"the  tid is: " + str(LastSeqNo_tid))

            index_in_log = findIndex_SeqNuM(LastSeqNo)
            index_in_los = findIndex_InLosers(loser,LastSeqNo_tid)

            if(logBuffer[index_in_log]["actiontype"] == "compensation"):
                printt("the action is compensation")
                loser[index_in_los][1]=logBuffer[index_in_log]["NextUndoSeqNo"]

            if(logBuffer[index_in_log]["actiontype"] == "write"):
                printt("the action is write")
                pageNum = logBuffer[index_in_log]["page"]
                if(checkPageInCache(pageNum) == -1):
                    fetch(pageNum)
                
                if(cache[checkPageInCache(pageNum)]["page"]["psn"] >= LastSeqNo):
                    inverse(logBuffer[index_in_log])
                loser[index_in_los][1]=logBuffer[index_in_log]["PreviousSN"]

            if(logBuffer[index_in_log]["actiontype"] == "begin"):
                printt("the action is begin")
                logentry={"lsn": sequence, "actiontype":"rollback", "tid": LastSeqNo_tid,"PreviousSN":checkLSNofTrans(LastSeqNo_tid)}
                logBuffer.append(logentry)
                printt("trans=" + str(LastSeqNo_tid) + "is undo. \n" +"creating new log entry with " + str(logentry))
                loser.remove([LastSeqNo_tid, LastSeqNo])
                printt("trans=" + str(LastSeqNo_tid)  + " is now not in Activators \n")
            sequence+=1
            force()

#get a list of losers tranactions id
#return the trancaction with maximum sequnce, and maximum sequnce
def findMaxSeq_InLosers(loser): 
    LastSeqNo=-1
    LastSeqNo_tid=-1
    for los in loser:
        if(LastSeqNo< int(los[1])):
            LastSeqNo_tid = int(los[0])
            LastSeqNo = int(los[1])
    return LastSeqNo_tid, LastSeqNo
#param, array of losers transaction id
#return the index of losers with value as param 
def findIndex_InLosers(loser,tid):
    counter=0
    for los in loser:
        if(los[0] == tid):
            return counter
        counter+=1
    return -1
#controling function used for demonstrate easy
def startFunction():
    global line
    global forceDeleteLogBuffer
    print("Enter f if it is 'First Use'     or       r if it is 'Recover Mode'")
    read=input()
    if(read == "f"):
        createStableStorage()
        readFile("SENARIO3.txt")
    else:
        forceDeleteLogBuffer=False
        redo()
        line+=1
        printt('redo finished!')
        undo()
        line+=1
        printt('    Recover Completed!!')
        forceDeleteLogBuffer=True

startFunction()

