# -*- coding: UTF-8 -*-
import time
import uuid
import logging
import socket
import sys
import os
import random
import json
# 访问activemq
import stomp
# 访问zookeeper
from kazoo.client import KazooClient
# 定时任务
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
# 通讯rpyc模块
import rpyc 
from rpyc.utils.server import ThreadedServer
# 获取硬件信息
import psutil

from config import *
from libraries import *

# 当前worker的id
workerid=''
if STATIC_CONFIGS.has_key('WORK_ID') == True:
    workerid = STATIC_CONFIGS['WORK_ID']
if workerid == '':
    workerid = uuid.uuid4()

# 初始化变量
logLevel=logging.INFO
if STATIC_CONFIGS.has_key('LOGS') == True:
    if STATIC_CONFIGS['LOGS']['LEVEL'] == 'DEBUG':
        logLevel=logging.DEBUG
    elif STATIC_CONFIGS['LOGS']['LEVEL'] == 'INFO':
        logLevel=logging.INFO
    elif STATIC_CONFIGS['LOGS']['LEVEL'] == 'WARN':
        logLevel=logging.WARN
    else:
        logLevel=logging.ERROR
else:
    logLevel=logging.ERROR

RPYC_SECRET_KEY=STATIC_CONFIGS['RPYCS']['SECRET_KEY']
RPYC_HOST = ''
RPYC_PORT = ''
RPYC_WEIGHT='1'
if STATIC_CONFIGS['RPYCS'].has_key('HOST') == True:
    RPYC_HOST=STATIC_CONFIGS['RPYCS']['HOST']

if STATIC_CONFIGS['RPYCS'].has_key('PORT') == True:
    RPYC_PORT=STATIC_CONFIGS['RPYCS']['PORT']

if STATIC_CONFIGS['RPYCS'].has_key('WEIGHT') == True:
    RPYC_WEIGHT=STATIC_CONFIGS['RPYCS']['WEIGHT']

# 不指定启动机器及端口，则随机生成
if RPYC_HOST == '':
    RPYC_HOST = socket.gethostbyname(socket.gethostname())

if RPYC_PORT == '':
    RPYC_PORT = random_port(20000,25000, 10)

ZOOKEEPER_HOSTS='127.0.0.1:2181'
ZOOKEEPER_PARENT_PATH='/test'
if STATIC_CONFIGS.has_key('ZOOKEEPERS') == True:
    if STATIC_CONFIGS['ZOOKEEPERS'].has_key('HOSTS') == True:
        ZOOKEEPER_HOSTS = STATIC_CONFIGS['ZOOKEEPERS']['HOSTS']
    if STATIC_CONFIGS['ZOOKEEPERS'].has_key('START_PATH') == True:
        ZOOKEEPER_PARENT_PATH = STATIC_CONFIGS['ZOOKEEPERS']['START_PATH']

ACTIVEMQ_HOSTS='127.0.0.1:61613'
ACTIVEMQ_USER=''
ACTIVEMQ_PASSWORD=''
RECONNECT_SLEEP_INTEVAL=60
if STATIC_CONFIGS.has_key('ACTIVEMQ') == True:
    if STATIC_CONFIGS['ACTIVEMQ'].has_key('HOSTS') == True:
        ACTIVEMQ_HOSTS=STATIC_CONFIGS['ACTIVEMQ']['HOSTS']
    if STATIC_CONFIGS['ACTIVEMQ'].has_key('USER') == True:
        ACTIVEMQ_USER=STATIC_CONFIGS['ACTIVEMQ']['USER']
    if STATIC_CONFIGS['ACTIVEMQ'].has_key('PASSWORD') == True:
        ACTIVEMQ_PASSWORD=STATIC_CONFIGS['ACTIVEMQ']['PASSWORD']
    if STATIC_CONFIGS['ACTIVEMQ'].has_key('RECONNECT_SLEEP_INTEVAL') == True:
        RECONNECT_SLEEP_INTEVAL=STATIC_CONFIGS['ACTIVEMQ']['RECONNECT_SLEEP_INTEVAL']

ACTIVEMQ_HOSTS_LISTS = as_activemq_hosts_list(ACTIVEMQ_HOSTS)

WATCHER_SLEEP_INTEVAL=60
LEADER_AUTO_BALANCE=False
if STATIC_CONFIGS.has_key('WATCHER') == True:
    if STATIC_CONFIGS['WATCHER'].has_key('SLEEP_INTEVAL') == True:
        WATCHER_SLEEP_INTEVAL=STATIC_CONFIGS['WATCHER']['SLEEP_INTEVAL']

    if STATIC_CONFIGS['WATCHER'].has_key('BALANCE') == True:
        if STATIC_CONFIGS['WATCHER']['BALANCE'].has_key('AUTO') == True:
            LEADER_AUTO_BALANCE=STATIC_CONFIGS['WATCHER']['BALANCE']['AUTO']

# 当前pid
pid= os.getpid()
# 初始化日志
logging.basicConfig(level=logLevel, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',filename=sys.path[0]+'/logs/worker_' + str(workerid) + '.log', filemode='a')
logger = logging.getLogger()

# 初始化定时器
scheduler = BackgroundScheduler()
scheduler.start()
# activemq链接
mqClient = None
# zookeeper链接
zkClient = None
# rpycserver链接
rpycserver = None

UnhandledInterruptHappend=False
# default-encoding
reload(sys)
sys.setdefaultencoding('utf-8')
sysdir=os.path.abspath(os.path.dirname(__file__))

gTaskCount = 0
def connect_and_subscribe(conn):
    conn.start()
    if not ACTIVEMQ_USER and ACTIVEMQ_USER <> '':
        vprint('connect activemq with auth!', None, logger, logging.INFO)
        conn.connect(ACTIVEMQ_USER, ACTIVEMQ_PASSWORD, wait=True)
    else:
        conn.connect()

# activemq的消息listener
class ActivemqMsgListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_error(self, headers, message):
        vprint('activemq received an error "%s"',(message,), logger, logging.ERROR)

    def on_connected(self, headers, body):
        vprint('connect to activemq',  None,logger, logging.INFO)

    def on_disconnected(self):
        vprint('disconnect to activemq', None, logger, logging.INFO)
        global UnhandledInterruptHappend
        if UnhandledInterruptHappend == False:
            time.sleep(RECONNECT_SLEEP_INTEVAL)
            vprint('reconnect to activemq', None,logger, logging.INFO)
            connect_and_subscribe(self.conn)
        else:
            pass

# 定时触发，提交任务到mq
def execute_task_submit(json_str):
    global mqClient
    vprint('submit task to activemq ! body: %s',(json_str,), logger, logging.DEBUG)
    mqClient.send(body=json_str, destination='/queue/task')

# 追加定时任务
def vadd_job(cronobj, jstr):
    global scheduler
    vprint('add_job corn:%s jstr:%s' ,(str(cronobj), jstr), logger, logging.INFO)

    if cronobj['type'] == 'interval':
        # 间隔式任务
        scheduler.add_job(execute_task_submit, 'interval', seconds=int(cronobj['value']), args=[jstr])
    elif cronobj['type'] == 'date':
        # 只执行一次的时间式任务
        scheduler.add_job(execute_task_submit, 'date', run_date=cronobj['value'], args=[jstr])
    else: 
        # cron表达式任务
        jobject = json.loads(cronobj['value'])
        lyear=None
        lmonth=None
        lday=None
        lweek=None
        lday_of_week=None
        lhour=None
        lminute=None
        lsecond=None
        lstart_date=None
        lend_date=None
        if jobject.has_key('year'):
            year = jobject['year']
        if jobject.has_key('month'):
            lmonth = jobject['month']
        if jobject.has_key('day'):
            lday = jobject['day']
        if jobject.has_key('week'):
            lweek = jobject['week']
        if jobject.has_key('day_of_week'):
            lday_of_week = jobject['day_of_week']
        if jobject.has_key('hour'):
            lhour = jobject['hour']
        if jobject.has_key('minute'):
            lminute = jobject['minute']
        if jobject.has_key('second'):
            lsecond = jobject['second']
        if jobject.has_key('start_date'):
            lstart_date = jobject['start_date']
        if jobject.has_key('end_date'):
            lend_date = jobject['end_date']
        scheduler.add_job(execute_task_submit, 'cron', year=lyear, month=lmonth, day=lday,week=lweek,day_of_week=lday_of_week, \
            hour=lhour,minute=lminute,second=lsecond,start_date=lstart_date,end_date=lend_date,args=[jstr])

# rpyc服务
class RpycWorkerService(rpyc.Service):
    # 登陆
    def exposed_login(self,user,passwd):
        if user=="OMuser" and passwd=="KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h":
            self.Checkout_pass=True
        else:
            self.Checkout_pass=False

    # 定时任务同步
    def exposed_SyncTaskerStart(self):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")

        # 暂停并删除定时器
        global scheduler
        if scheduler.running == True:
            scheduler.pause()
            scheduler.remove_all_jobs()

        # 任务数清零
        global gTaskCount
        gTaskCount=0
        return self.response("C_0000", "sucess")

    def exposed_SyncTaskerComplete(self):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")

        # 开始定时器
        global scheduler
        if scheduler.running == True:
            scheduler.resume()
        return self.response("C_0000", "sucess")

    def exposed_SyncTaskerInfo(self,message): 
        global gTaskCount
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
            dmessage = tdecode(message,RPYC_SECRET_KEY)
            jobject = json.loads(dmessage)
            # 获取cron表达式
            jcron = json.loads(jobject[0]['module_cron'])
            # 替换from
            for i in range(0,len(jobject)):
                jobject[i]['from'] = str(workerid)
            vadd_job(jcron, json.dumps(jobject))
            gTaskCount+=1
        except Exception, e:
            vprint('[%s]received a exception %s', ('SyncTaskerInfo',str(e),) + dmessage, logger, logging.ERROR)
            return self.response("C_9098", str(e))

        return self.response("C_0000", "sucess")

    def exposed_QueryMemberInfo(self):
        global gTaskCount
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")
        #环境信息  
        infos={}
        infos['count']=str(gTaskCount)
        infos['pid']=str(os.getpid())
        try:
            # 获取硬件信息
            import psutil
            global pid
            cprocess=psutil.Process(pid)
            infos['memory']=str("%.2f M" % (psutil.virtual_memory().total/(1024*1024)))
            infos['memory_percent']=str(psutil.virtual_memory().percent) + '%'
            infos['pid_memory_percent']="%.2f%%" % (cprocess.memory_percent())
            infos['cpu_percent']=str(psutil.cpu_percent(0.5)) + '%'
            infos['pid_cpu_percent']=str(cprocess.cpu_percent()) + '%'
        except Exception, e:
            vprint('QueryMemberInfo exception: %s', (str(e),), logger, logging.DEBUG)
        return self.response("C_0000", json.dumps(infos))

    def exposed_Shutdownit(self, workid):
        try:
            if self.Checkout_pass == True:
                dworkid = tdecode(workid,RPYC_SECRET_KEY)
                vprint('server is closed by : %s', (str(dworkid),), logger, logging.INFO)
                global rpycserver
                rpycserver.close()
                rpycserver=None
            else:
                pass
        except Exception, e:
            vprint('[%s]received a exception %s', ('RpycService Shutdown',str(e),), logger, logging.ERROR)


    def response(self,code, message):
        dict={}
        dict['code'] = str(code)
        dict['msg'] = str(message)
        dictstr = json.dumps(dict);

        vprint('response: %s' ,(dictstr,), logger, logging.DEBUG)
        return tencode(dictstr,RPYC_SECRET_KEY)
def set_interrupt_happend():
    global UnhandledInterruptHappend
    UnhandledInterruptHappend = True

if __name__ == '__main__':
    try:
        vprint('task %s is start!' , (str(workerid),), logger, logging.INFO)
        vprint('connect to zookeeper hosts:=%s!' , (ZOOKEEPER_HOSTS,), logger, logging.INFO)
        # 连接zookeeper
        zkClient = KazooClient(hosts=ZOOKEEPER_HOSTS)
        zkClient.start()

        # 确认路径，如果有必要则创建该路径
        zkClient.ensure_path(ZOOKEEPER_PARENT_PATH + "/leader/worker")
        zkClient.ensure_path(ZOOKEEPER_PARENT_PATH + "/leader/worker/path")
        # 创建zk节点
        zkClient.create(ZOOKEEPER_PARENT_PATH + "/leader/worker/" + str(RPYC_HOST) + '_' + str(RPYC_PORT), value=str(RPYC_WEIGHT), ephemeral=True)
        zkClient.create(ZOOKEEPER_PARENT_PATH + "/leader/worker/path/" + str(RPYC_HOST) + '_' + str(RPYC_PORT), value=sysdir.decode("GBK").encode("utf-8"), ephemeral=True)
        vprint('connect to activemq hosts:=%s!' ,ACTIVEMQ_HOSTS, logger, logging.INFO)
        mqClient = stomp.Connection(ACTIVEMQ_HOSTS_LISTS, heartbeats=(8000, 8000))
        mqClient.set_listener('ActivemqMsgSenderListener', ActivemqMsgListener(mqClient))
        connect_and_subscribe(mqClient)

        vprint('start rpyc connection thread! host:%s port:%s' , (str(RPYC_HOST),str(RPYC_PORT)), logger, logging.INFO)
        rpycserver=ThreadedServer(RpycWorkerService, port=int(RPYC_PORT),auto_register=False)
        rpycserver.start()
    except Exception, e:
        vprint('[%s]received a exception %s', ('main',str(e),), logger, logging.ERROR)
    finally:
        set_interrupt_happend()
        try:
            if zkClient is not None:
                zkClient.stop()
        except:
            pass
        try:
            if mqClient is not None:
                mqClient.disconnect()
        except:
            pass
        try:
            if scheduler is not None:
                scheduler.shutdown()
        except:
            pass
        try:
            if rpycserver is not None:
                rpycserver.close()
        except:
            pass
        vprint('task %s is stop!', (str(workerid),), logger, logging.INFO)