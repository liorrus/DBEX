import os
import json

cacheGeneralCounter = 0
cache=[{},{},{}]

#create a empty Stable Storage
def createStableStorage():
    for i in range(1,21):
        if (os.path.isfile('./stablestorage/'+ str(i)) == False):
            print("kakif " + str(i))
            page = {}
            page["id"]=i
            page["psn"]=0
            page["content"]=[None]*20
            json.dump(page, open('./stablestorage/'+ str(i),"w+"))


# checks if a page is in cache and return where in cache, if not in cache return -1
def checkPageInCache(id):
    print("checkig if page on cache")
    for i in range (0,3):
        if(cache[i]!={} and cache[i]["page"]["id"] == id):
            print("page found on i=" + str(i))
            return i
    print("page not in cache")
    return -1

def fetch(id):
    print("start fetching page=" + str(id))
    global cacheGeneralCounter
    global cache
    lru=LRU()
    if(cache[lru] != {}):
        print("the cache is full writing lru=" + str(lru))
        write(lru)
    cache[lru]["page"]=json.load(open('./stablestorage/'+ str(id)))
    cache[lru]["page"]["status"] = 0
    cache[lru]["cacheCounter"] = cacheGeneralCounter
    cacheGeneralCounter += 1
    return lru

#need to do as on algo this only temp"
def write(i):
    print("writing page cache from where i=" + str(i) + "page=" + str(cache[i]["page"]["id"]))
    page={}
    page["id"] = cache[i]["page"]["id"]
    page["psn"] = cache[i]["page"]["psn"]
    page["content"]=cache[i]["page"]["content"]
    json.dump(page, open('./stablestorage/'+ str(cache[i]["page"]["id"]),"w+"))


#return the page of least recently used 
def LRU():
    for i in range(0,3):
        if(cache[i]=={}):
            print("empty cache found on cache page i=" +str(i))
            return i
    minimum = min(cache[0]["cacheCounter"],cache[1]["cacheCounter"],cache[2]["cacheCounter"])
    for i in range(0,3):
        if(cache[i]["cacheCounter"]==minimum):
            print("least used page is in cache pag i="+ str(i))
            return i

def Update(id,length,offset,value):
    global cache
    global cacheGeneralCounter
    print("checking if page on cache")
    cached=checkPageInCache(id)
    if(cached == -1):
        print("page not on cache")
        cached=fetch(id)
        print("page is now on cache at i=" + str(cached))
    else:
        print("page on cache at i=" + str(cached))
    print("change content and psn")
    for i in range(offset,offset+length):
        cache[cached]["page"]["content"][i]=value[i-offset]
        cacheGeneralCounter+=1
        cache[cached]["cacheCounter"]=cacheGeneralCounter
    cache[cached]["page"]["psn"]+=1   
    cache[cached]["page"]["status"]=1
    print("update finished")



#checkPageInCache(5)
#fetch(5)
#fetch(6)
#fetch(7)
#fetch(8)
#checkPageInCache(5)
#checkPageInCache(8)
createStableStorage()
Update(5,2,2,"aa")
Update(6,2,2,"bb")
Update(7,2,2,"cc")
Update(5,2,3,"dd")
Update(8,2,2,"ee")