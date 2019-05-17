import os
import json

cacheGeneralCounter = 0
cache=[{},{},{}]
logBuffer=[]
line=0
jumpToNextLine=0
activetrans=[]
sequence = 1
#create a empty Stable Storage
def createStableStorage():
    for i in range(1,21):
        if (os.path.isfile('./stablestorage/'+ str(i)) == False):
            printt("kakif " + str(i))
            page = {}
            page["id"]=i
            page["psn"]=0
            page["content"]=[None]*20
            json.dump(page, open('./stablestorage/'+ str(i),"w+"))
    if(os.path.isfile('./stablestorage/stablelog') == False):
        open('./stablestorage/stablelog',"ab")



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
    for log in logBuffer:
        try: 
            x=log["page"]
            if(x==pageid):
                return True
        except:
            pass
    return False

#need to do as on algo this only temp"
def flushByCacheLocation(i):
    global cache
    printt("checking if the page has a relevant log entry")
    if(checkForLogEntry(cache[i]["page"])==True):
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
        printt("writing log entry with params " + log)
        json.dump(log, open('./stablestorage/stablelog',"a+"))
    printt("finished writing logBuffer to stable log")
    logBuffer=[]
    printt("force completed")


def commit():
    pass

def begin(tid):
    global sequence
    printt("adding trans=" + str(tid) + "activetrans")
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

def write(tid,id,length,offset,value):
    global cache
    global cacheGeneralCounter
    global activetrans
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
    for i in range(offset,offset+length):
        cache[cached]["page"]["content"][i]=value[i-offset]
        cacheGeneralCounter+=1
        cache[cached]["cacheCounter"]=cacheGeneralCounter
    cache[cached]["page"]["psn"]+=1   
    cache[cached]["page"]["status"]=1
    logEntry={}
    #logEntry["LSN"]=
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
    
    if(parsed[0] =="n"): 
        return
    elif(parsed[0] == "M"): 
        printCachePage(int(parsed[1]))
        printt()
    elif(parsed[0]=="c"):
        jumpToNextLine = line+1
    elif(parsed[0]=="J"):
        jumpToNextLine = int(parsed[1])

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

#checkPageInCache(5)
#fetch(5)
#fetch(6)
#fetch(7)
#fetch(8)
#checkPageInCache(5)
#checkPageInCache(8)
createStableStorage()
line=1
write(1,5,2,2,"aa")
line=2
write(2,6,2,2,"bb")
line=3
write(2,7,2,2,"cc")
line=4
write(1,1,2,3,"dd")
line=5
write(3,3,2,2,"ee")