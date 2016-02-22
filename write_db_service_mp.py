#-*- coding: utf-8 -*-
import sys, os, time, traceback, datetime
import cPickle as pickle
from zlib import decompress
import sys, os.path, traceback
from Queue import Empty
from multiprocessing import Process, Queue, Value

import logging
import daemon

import time

log = logging.getLogger('daemon')
hdlr = logging.FileHandler("/opt/sites/dbw/write_db_service_mp.log")
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%T')
hdlr.setFormatter(formatter)
log.addHandler(hdlr)
log.setLevel(logging.DEBUG)

LOCAL = False
if LOCAL:
    sys.path.insert(0, 'D:/PyDev/farm_lib-b_dbw')                   #farm_lib
    sys.path.insert(0, 'D:/PyDev/xn_farm.rekoo.com/src')            #webapp
else:
    sys.path.insert(0, '/opt/sites/mxland.rekoo.jp')



from rklib.core import app
cur_dir = '/opt/sites/mxland.rekoo.jp'

import apps.settings as settings
from django.core.management import setup_environ
setup_environ(settings)

app.debug = settings.DEBUG
app.init(storage_cfg_file=cur_dir + "/apps/config/storage.conf", logic_cfg_file=cur_dir + "/apps/config/logic.conf",
         model_cfg_file=cur_dir + "/apps/config/model.conf", cache_cfg_file=cur_dir + "/apps/config/cache.conf")

from rklib.client.tokyotyrant import TTClient
from rklib.client.mcache import MemcacheClient

 
ttclient = TTClient(app.storage_engines['ttmc'].servers)
memclient = MemcacheClient(app.storage_engines['memcache'].servers)

#-------------------------------------------------------------------------------
_class_cache_ = {}
def _getClsByCacheKey(cache_key):
    '''
    根据cache_key获取类对象
    '''
    mc_name = cache_key[cache_key.find("|")+1:cache_key.rfind("|")]
    myclass = _class_cache_.get(mc_name)
    if myclass:
        return myclass
    else:
        pos = mc_name.rfind(".")
        module_name = mc_name[:pos]
        class_name = mc_name[pos+1:]
        mod = __import__(module_name, globals(), locals(), [class_name], -1)
        myclass = getattr(mod, class_name)
        _class_cache_[mc_name] = myclass
    return myclass

#-------------------------------------------------------------------------------
def _writeDB_clearTT(key, count):
    res = memclient.get(key)
    if res:
        myclass = _getClsByCacheKey(key)
        data = pickle.loads(res)
        obj = myclass.loads(data)
        obj.need_insert= False
        obj.put_only_bottom()
    ttclient.delete(key)

#-------------------------------------------------------------------------------
def writeWorker(q, count):
        while True:
            try:
                key = q.get(1)
                if key:
                    _writeDB_clearTT(key, count)
                    count.value += 1
            except Empty:
                time.sleep(5)
            except:
                log.error("key:" + key)
                log.error(sys.exc_info())


#-------------------------------------------------------------------------------                            
def writeToDB(workQueue):
    log.info("writeToDB begin at %s", datetime.datetime.now())
    total_count = 0
    for cache_key in ttclient.iterkeys():
        print cache_key
        workQueue.put(cache_key)  
        total_count += 1
        if (total_count % 10000) == 0:
            log.info("Read %d keys from ttserver.", total_count)
    ttclient.close()
    log.info("writeToDB done. read %d keys from ttserver. ", total_count)

#-------------------------------------------------------------------------------
def showStatus(q, count): 
    while True:
        log.info("queue_size:%d  write_to_db_count:%d" % (q.qsize(), count.value))
        count.value = 0
        time.sleep(10)  
        #ttclient.close()
                        
#-------------------------------------------------------------------------------
def main():
    print "Start!"
    daemon.daemonize(noClose=True)
    #初始化工作线程
    num_of_workers = 4
    workers = []
    workQueue = Queue()
    writeCount = Value("i", 0)
    for i in range( num_of_workers ):  
        worker = Process( target=writeWorker, args=(workQueue, writeCount))  
        workers.append(worker)
    for worker in workers:
        worker.start()
    
    sp = Process(target=showStatus, args=(workQueue, writeCount))
    sp.start()
    
    #定期从ttserver读取要保存的数据        
    while True:
        btime = time.time()
        try:
            writeToDB(workQueue)
        except:
            pass
        etime = time.time()
        log.info("Use time: %f", etime - btime)
        stime = 600 - (etime - btime)
        if stime >0:
            time.sleep(stime)
    
if __name__ == "__main__":
    main()


