#coding: utf-8
import redis
import sys
import os
from ssdb import SSDB
import time
import multiprocessing
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

SSDB_HOST="192.168.217.130"
SSDB_PORT=42222
STEP_NUM= 100
EX_TIME=30 * 86400
EX_KEY="mrpexpire"
#set expire
def setExpire(keyList,redisType, extime):
	if redisType not in ["string", "list", "set", "hash", "sortedset"] or len(keyList) == 0:
		print "key type error! type: %s" %(redisType)
		return
	
	data = {}	
	data[EX_KEY] = {}
	for key in keyList:
		stringKey = "%s_%s" %(redisType, key)
		data[EX_KEY][stringKey] = int(time.time()) + extime 
		print "expire key: %s" %(stringKey)
	print "get expire key size: %d" %(len(data))
	#print data
	#put data to ssdb
	s = SSDB(SSDB_HOST, SSDB_PORT)
	for key in data:
		s.multi_zset(key, **data[key])
		print "zadd key: %s" % (key)
	print "set expire key finish"

#get data from redis and put to ssdb -- string
def processString(arg):
	keyList = arg[0] 
	redisHost= arg[1] 
	redisPort= arg[2] 
	ssdbHost= arg[3]
	ssdbPort = arg[4]

	if len(keyList) == 0:
		return
	try:
		#get data from redis
		data = {}
		r = redis.Redis(host=redisHost, port=redisPort)
		#get keys
		retList = r.mget(keyList)
		for i in range(len(retList)):
			if retList[i] != "(nil)":
				data[keyList[i]] = retList[i]	
		#put keys
		print "get redis data size: %d" %(len(data))
		#put data to ssdb
		s = SSDB(SSDB_HOST, SSDB_PORT)
		for key in data:
			s.setx(key, data[key], EX_TIME)
			print "set key: %s" % (key)
	except Exception ,e:
		print e.message	

#split data
def splitData(keyList, redisHost, redisPort, ssdbHost, ssdbPort):
	if len(keyList) == 0:
		return []
	else:
		#split data
		step = len(keyList) / STEP_NUM
		dataArr = []
		for i in range(step):
			#get keys
			dataArr.append([keyList[i * STEP_NUM: (i + 1) * STEP_NUM]])
			dataArr[-1].extend([redisHost, redisPort, ssdbHost, ssdbPort])
		dataArr.append([keyList[step * STEP_NUM: ]])
		dataArr[-1].extend([redisHost, redisPort, ssdbHost, ssdbPort])
		return dataArr
#string
def copyString(keyList, redisHost, redisPort, ssdbHost, ssdbPort):
	if len(keyList) == 0:
		return
	try:
		#split data
		dataArr = splitData(keyList, redisHost, redisPort, ssdbHost, ssdbPort)	
		print "get data size: %d" %(len(dataArr))
		keyList = []		
		#process data
		pool = multiprocessing.Pool(multiprocessing.cpu_count())
		pool.map(processString, dataArr)
		pool.close()
		pool.join()
		
	except Exception ,e:
		print e.message	

#get data from redis and put to ssdb -- list
def processList(arg):
	keyList = arg[0] 
	redisHost= arg[1] 
	redisPort= arg[2] 
	ssdbHost= arg[3]
	ssdbPort = arg[4]

	if len(keyList) == 0:
		return
	try:
		#get data from redis
		data = {}
		r = redis.Redis(host=redisHost, port=redisPort)
		pipeRedis = r.pipeline()
		#get keys
		for item in keyList:
			pipeRedis.lrange(item, 0, -1)
		retList = pipeRedis.execute()
		for i in range(len(retList)):
			if len(retList[i]) > 0:
				data[keyList[i]] = retList[i]	
		#put keys
		print "get redis data size: %d" %(len(data))
		#print data
		#put data to ssdb
		s = SSDB(SSDB_HOST, SSDB_PORT)
		for key in data:
			s.qclear(key)
			s.qpush_back(key, *data[key])
			print "lpush key: %s" % (key)
		#set expire
		setExpire(keyList,"list", EX_TIME)
	except Exception ,e:
		print e.message	

#list
def copyList(keyList, redisHost, redisPort, ssdbHost, ssdbPort):
	if len(keyList) == 0:
		return
	try:
		#split data
		dataArr = splitData(keyList, redisHost, redisPort, ssdbHost, ssdbPort)	
		print "get data size: %d" %(len(dataArr))
		keyList = []		
		#process data
		pool = multiprocessing.Pool(multiprocessing.cpu_count())
		pool.map(processList, dataArr)
		pool.close()
		pool.join()

	except Exception ,e:
		print e.message	

#get data from redis and put to ssdb -- list
def processSet(arg):
	keyList = arg[0] 
	redisHost= arg[1] 
	redisPort= arg[2] 
	ssdbHost= arg[3]
	ssdbPort = arg[4]

	if len(keyList) == 0:
		return
	try:
		#get data from redis
		data = {}
		r = redis.Redis(host=redisHost, port=redisPort)
		pipeRedis = r.pipeline()
		#get keys
		for item in keyList:
			pipeRedis.smembers(item)
		retList = pipeRedis.execute()
		#print retList
		for i in range(len(retList)):
			if len(retList[i]) > 0:
				data[keyList[i]] = {}
				for item in retList[i]:
					data[keyList[i]][item] = 0
		#put keys
		print "get redis data size: %d" %(len(data))
		#print data
		#put data to ssdb
		s = SSDB(SSDB_HOST, SSDB_PORT)
		for key in data:
			s.zclear(key)
			s.multi_zset(key, **data[key])
			print "zadd key: %s" % (key)

		#set expire
		setExpire(keyList,"sortedset", EX_TIME)
	except Exception ,e:
		print e.message	


#set
def copySet(keyList, redisHost, redisPort, ssdbHost, ssdbPort):
	if len(keyList) == 0:
		return
	try:
		#split data
		dataArr = splitData(keyList, redisHost, redisPort, ssdbHost, ssdbPort)	
		print "get data size: %d" %(len(dataArr))
		keyList = []	
		#process data
		pool = multiprocessing.Pool(multiprocessing.cpu_count())
		pool.map(processSet, dataArr)
		pool.close()
		pool.join()

	except Exception ,e:
		print e.message	

#get data from redis and put to ssdb -- hash
def processHash(arg):
	keyList = arg[0] 
	redisHost= arg[1] 
	redisPort= arg[2] 
	ssdbHost= arg[3]
	ssdbPort = arg[4]

	if len(keyList) == 0:
		return
	try:
		#get data from redis
		data = {}
		r = redis.Redis(host=redisHost, port=redisPort)
		pipeRedis = r.pipeline()
		#get keys
		for item in keyList:
			pipeRedis.hgetall(item)
		retList = pipeRedis.execute()
		
		for i in range(len(retList)):
			if len(retList[i]) > 0:
				data[keyList[i]] = retList[i]
		
		#put keys
		print "get redis data size: %d" %(len(data))
		#print data
		#put data to ssdb
		s = SSDB(SSDB_HOST, SSDB_PORT)
		for key in data:
			s.hclear(key)
			s.multi_hset(key, **data[key])
			print "hset key: %s" % (key)
		#set expire
		setExpire(keyList,"hash", EX_TIME)
	except Exception ,e:
		print e.message	


#hash
def copyHash(keyList, redisHost, redisPort, ssdbHost, ssdbPort):
	if len(keyList) == 0:
		return
	try:
		#split data
		dataArr = splitData(keyList, redisHost, redisPort, ssdbHost, ssdbPort)	
		print "get data size: %d" %(len(dataArr))
		keyList = []	
		#process data
		pool = multiprocessing.Pool(multiprocessing.cpu_count())
		pool.map(processHash, dataArr)
		pool.close()
		pool.join()

	except Exception ,e:
		print e.message	


#get data from redis and put to ssdb -- list
def processSortedset(arg):
	keyList = arg[0] 
	redisHost= arg[1] 
	redisPort= arg[2] 
	ssdbHost= arg[3]
	ssdbPort = arg[4]

	if len(keyList) == 0:
		return
	try:
		#get data from redis
		data = {}
		r = redis.Redis(host=redisHost, port=redisPort)
		pipeRedis = r.pipeline()
		#get keys
		for item in keyList:
			pipeRedis.zrange(item, 0, -1,withscores=True)
		retList = pipeRedis.execute()
		for i in range(len(retList)):
			if len(retList[i]) > 0:
				data[keyList[i]] = {}
				for item in retList[i]:
					#扩大权重，因为小数会被ssdb置为0
					data[keyList[i]][item[0]] = 1000*item[1]
		#put keys
		print "get redis data size: %d" %(len(data))
		#put data to ssdb
		s = SSDB(SSDB_HOST, SSDB_PORT)
		for key in data:
			s.zclear(key)
			s.multi_zset(key, **data[key])
			print "zadd key: %s" % (key)
		#set expire
		setExpire(keyList,"sortedset", EX_TIME)
	except Exception ,e:
		print e.message	

#sortedset
def copySortedset(keyList, redisHost, redisPort, ssdbHost, ssdbPort):
	if len(keyList) == 0:
		return
	try:
		#split data
		dataArr = splitData(keyList, redisHost, redisPort, ssdbHost, ssdbPort)	
		print "get data size: %d" %(len(dataArr))
		keyList = []	
		#process data
		pool = multiprocessing.Pool(multiprocessing.cpu_count())
		pool.map(processSortedset, dataArr)
		pool.close()
		pool.join()

	except Exception ,e:
		print e.message	


if __name__ == "__main__":
	fileName = str(sys.argv[1])
	redis_host= sys.argv[2]
	redis_port= int(sys.argv[3])
	beginNum = int(sys.argv[4])
	print fileName, redis_host, redis_port, beginNum
	with open(fileName,"r") as fp:
		stringArr, listArr, setArr, hashArr, sortedsetArr = [], [], [], [], []
		lineNum = 0
		for line in fp:
			lineNum += 1
			if lineNum < beginNum: 
				continue
			arr = line.strip().split(",")
			if len(arr) != 8 or arr[0]  != "0":
				print "err: line[%s]" %(line.strip())
				continue
			else:
				redisType = arr[1]
				redisKey = arr[2]
				if redisType == "string":
					stringArr.append(redisKey)
				elif redisType == "list":
					listArr.append(redisKey)
				elif redisType == "set":
					setArr.append(redisKey)
				elif redisType == "hash":
					hashArr.append(redisKey)
				elif redisType == "sortedset":
					sortedsetArr.append(redisKey)
				else:
					print "key type error! line[%s]" %(line.strip())
	try:
		start= int(time.time())
		copyString(stringArr, redis_host, redis_port, SSDB_HOST, SSDB_PORT)
		end = int(time.time())
		print "process string cost %d" %(end - start)
	except Exception,e:
		print e.message	
	stringArr = []
	try:
		start = end
		copyList(listArr, redis_host, redis_port, SSDB_HOST, SSDB_PORT)
		end = int(time.time())
		print "process list cost %d" %(end - start)
	except Exception,e:
		print e.message	
	listArr = []
	try:
		start = end
		copySet(setArr, redis_host, redis_port, SSDB_HOST, SSDB_PORT)
		end = int(time.time())
		print "process set cost %d" %(end - start)
	except Exception,e:
		print e.message	
	setArr = []
	
	try:	
		start = end
		copyHash(hashArr, redis_host, redis_port, SSDB_HOST, SSDB_PORT)
		end = int(time.time())
		print "process hash cost %d" %(end - start)
	except Exception,e:
		print e.message	
	hashArr = []

	try:	
		start = end
		copySortedset(sortedsetArr, redis_host, redis_port, SSDB_HOST, SSDB_PORT)
		end = int(time.time())
		print "process sorted set cost %d" %(end - start)
	except Exception,e:
		print e.message	

	sortedsetArr = []	
