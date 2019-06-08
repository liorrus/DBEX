import os
import json

cacheGeneralCounter = 0
cache=[{},{},{}]
logBuffer=[]
line=0
jumpToNextLine=0
activetrans=[]
sequence = 1
MasterRecord={} # StartPointer, LastCP
#create a empty Stable Storage
def createStableStorage():
    for i in range(1,21):
        if (os.path.isfile('./stablestorage/'+ str(i)) == False):
            page = {}
            page["id"]=i
            page["psn"]=0
            page["content"]=[None]*20
            json.dump(page, open('./stablestorage/'+ str(i),"w+"))
    if(os.path.isfile('./stablestorage/stablelog') == False):
        open('./stablestorage/stablelog',"ab")

def findIndex_SeqNuM(seq):
    counter=0
    for log in logBuffer:
        if(log["lsn"]==seq):
            return counter;
    return -1;

def abort(tid):
    global sequence
    global MasterRecord
    global activetrans
    global logBuffer

    LastSeqNo=findIndex_SeqNuM(checkLSNofTrans(tid))
    while(LastSeqNo!=None):
        if(logBuffer[LastSeqNo]["actiontype"] == "write"):
            inverse(logBuffer[LastSeqNo])
        LastSeqNo=logBuffer[LastSeqNo]["PreviousSN"]
    logentry={"lsn": sequence, "actiontype":"rollback", "tid": tid,"PreviousSN":checkLSNofTrans(tid)}
    logBuffer.append(logentry)
    printt("trans=" + tid  + "is aborted. \n" +
          "creating new log entry with " + str(logentry))
    activetrans.remove({"tid": tid, "lsn": checkLSNofTrans(tid)})
    printt("trans=" + str(tid)  + " is now not in Activators \n")
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

#need to do as on algo this only temp"
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
    cache[i]["page"]["status"] == "clean"
    
    json.dump(page, open('./stablestorage/'+ str(cache[i]["page"]["id"]),"w+"))
    printt("writing was completed")

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

def flush(pageid):
    for i in range(0,3):
        if(cache[i]["page"]["id"]==pageid):
            flushByCacheLocation(i)
            return
    printt("The cache has no page="+str(pageid))
    return

def force():
    global logBuffer
    printt("start forcing, writing logBuffer into stable log")
    for log in logBuffer:
        printt("writing log entry with params " + str(log))
        json.dump(log, open('./stablestorage/stablelog',"a+"))
    printt("finished writing logBuffer to stable log")
    logBuffer=[]
    printt("force completed")


def commit(tid):
    global sequence
    global activetrans
    global logBuffer
    printt("Remove trans= " + str(tid) + " from activetrans")
    activetrans.remove({"tid": tid, "lsn": checkLSNofTrans(tid)})
    logentry={"lsn": sequence, "actiontype":"commit", "tid": tid,"PreviousSN":None}
    printt("trans=" + str(tid)  + " is now not in Activators \n" + "creating new log entry with " + str(logentry))
    logBuffer.append(logentry)
    force()
    sequence+=1
    printt("commit finished")

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

def write(tid,id,length,offset,value,flag=1):
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
    for i in range(offset,offset+length):
        old_value=old_value+cache[cached]["page"]["content"][i]
        cache[cached]["page"]["content"][i]=value[i-offset]
        cacheGeneralCounter+=1
        cache[cached]["cacheCounter"]=cacheGeneralCounter
    lastpsn=cache[cached]["page"]["psn"]
    cache[cached]["page"]["psn"]=sequence   
    cache[cached]["page"]["status"]="dirty"
    logentry={"page": id, "lsn": sequence, "actiontype":"write", "tid": tid,"PreviousSN":lastpsn,"length":length, "offset":offset,"oldvValue":old_value}
    
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

def printStablePage(pageid):
    print(json.load(open('./stablestorage/'+ str(pageid))))

def printCachePage(pageid):
    print(cache)
    for i in range(0,3):
        if(cache[i]!={}):
            if(int(cache[i]["page"]["id"])==pageid):
                print(cache[i]["page"])
                return
    printt("there is no page on cache, with id="+str(pageid))
    return
    
def createLogEntry():
    pass

def readFile(filepath):
    global line
    with open(filepath) as fp:  
        row = fp.readline()
        while row:
            if "UPDATE" in row:
                parsed=row.split(":")
                tid=parsed[0]
                parsed1=parsed[1].split("UPDATE")
                parsed2=parsed1[1].split(",")
                write(int(tid), int(parsed2[0]), int(parsed2[1]), int(parsed2[2]), str(parsed2[3]))
            elif "COMMIT" in row:
                commit(int(row.split(":")[0]))
            elif "ABORT" in row:
                abort(int(row.split(":")[0]))
            elif "CHECKPOINT":
                checkpoint()
            row = fp.readline()
            line += 1

def readLogStable():
    stablelog=[]
    with open("./stablestorage/stablelog") as fp:
        row = fp.readline()
    parsed=row.split("}")
    for p in parsed[:-1]:
       temp=json.loads(str(p+"}"))
       stablelog.append(temp)
    return stablelog

def redo():
    global cacheGeneralCounter 
    global cache
    global logBuffer
    global sequence 
    printt("redo phase start reading stablelog")
    logBuffer=readLogStable()
    printt("stablelog is now at log buffer")
    for log in logBuffer():
        printt("checking if log type is write or rollback")
        if(log["actiontype"]=="write" or log["actiontpye"]=="compensation"):
            printt("log type is compensation or rollback, fetching page")    
            cached=fetch(log["page"])
            printt("page is now on cache page=" +str(cached) + " checking if page sequence is smaller then log sequence")
            if(cache[cached]["page"]["psn"]<log["sequence"]):
                printt("page sequence is smaller, redo log: checking if compansation or write")
                if(log["actiontype"]=="write"):
                    printt(" log action type is write, starting write to cache page")
                    for i in range(int(log["offset"]),int(log["offset"])+int(log["length"])):
                        cache[cached]["page"]["content"][i]=value[i-offset]
                        cacheGeneralCounter+=1
                        cache[cached]["cacheCounter"]=cacheGeneralCounter
                    cache[cached]["page"]["psn"]=log["sequence"]
                    cache[cached]["page"]["status"]="dirty"
                elif(log["actiontype"]=="compensation"):
                    printt("log type is compensation, starting compansation to cache page")
                    




     
 
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
        cache[cached]["page"]["content"][i]=logentry["oldValue"][i-logentry["offset"]]
        cacheGeneralCounter+=1
        cache[cached]["cacheCounter"]=cacheGeneralCounter
    lastpsn=cache[cached]["page"]["psn"]
    cache[cached]["page"]["psn"]=sequence   
    cache[cached]["page"]["status"]="dirty"
    logentry={"lsn": sequence, "actiontype":"compensation","PreviousSN":checkLSNofTrans(logentry["tid"])}
    for tran in activetrans:
        if(tran["tid"]==tid):
            tran["lsn"]=sequence
    logBuffer.append(logentry)
    sequence+=1
    printt("compansisiton finished" + str(logentry))


createStableStorage()
createStableStorage()
readFile("commands.txt")
