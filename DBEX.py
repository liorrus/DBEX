import os
import json

cacheGeneralCounter = 0
cache=[{},{},{}]
logBuffer=[]
line=0
jumpToNextLine=0
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

#need to do as on algo this only temp"
def flushByCacheLocation(i):
    printt("writing page cache from where i=" + str(i) + "page=" + str(cache[i]["page"]["id"]))
    page={}
    page["id"] = cache[i]["page"]["id"]
    page["psn"] = cache[i]["page"]["psn"]
    page["content"]=cache[i]["page"]["content"]
    json.dump(page, open('./stablestorage/'+ str(cache[i]["page"]["id"]),"w+"))

def flush(pageid):
    for i in range(0,3):
        if(cache[i]["page"]["id"]==pageid):
            flushByCacheLocation[i]
            return
    printt("The cache has no page="+str(pageid))
    return

def force():
    pass

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

def write(id,length,offset,value):
    global cache
    global cacheGeneralCounter
    printt("checking if page on cache")
    cached=checkPageInCache(id)
    if(cached
     == -1):
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
    


#checkPageInCache(5)
#fetch(5)
#fetch(6)
#fetch(7)
#fetch(8)
#checkPageInCache(5)
#checkPageInCache(8)
createStableStorage()
line=1
write(5,2,2,"aa")
line=2
write(6,2,2,"bb")
line=3
write(7,2,2,"cc")
line=4
write(5,2,3,"dd")
line=5
write(8,2,2,"ee")