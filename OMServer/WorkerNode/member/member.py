# -*- coding: UTF-8 -*-
import time
import uuid
import logging
import sys
import os
import random
import json
import threading
# 访问activemq
import stomp
# 访问zookeeper
from kazoo.client import KazooClient
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
    RPYC_PORT = random_port(25000,30000, 10)

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
if STATIC_CONFIGS.has_key('WATCHER') == True:
    if STATIC_CONFIGS['WATCHER'].has_key('SLEEP_INTEVAL') == True:
        WATCHER_SLEEP_INTEVAL=STATIC_CONFIGS['WATCHER']['SLEEP_INTEVAL']

    if STATIC_CONFIGS['WATCHER'].has_key('BALANCE') == True:
        if STATIC_CONFIGS['WATCHER']['BALANCE'].has_key('AUTO') == True:
            LEADER_AUTO_BALANCE=STATIC_CONFIGS['WATCHER']['BALANCE']['AUTO']

# 当前pid
pid= os.getpid()
# 初始化日志
logging.basicConfig(level=logLevel, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', filename=sys.path[0]+'/logs/worker_' + str(workerid) + '.log', filemode='a')
logger = logging.getLogger()
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

def connect_and_subscribe(conn):
    conn.start()
    if not ACTIVEMQ_USER and ACTIVEMQ_USER <> '':
        vprint('connect activemq with auth!', None, logger, logging.INFO)
        conn.connect(ACTIVEMQ_USER, ACTIVEMQ_PASSWORD, wait=True)
    else:
        conn.connect()
    # id 为queue或topic的uuid
    conn.subscribe(destination='/queue/task', id=str(100),ack='auto')

def response_result_to_mq(json_str):
    global mqClient
    #jsonobj = json.loads(json_str)
    #print 'try:' + jsonobj['response'][0].decode('utf-8')
    vprint('response to activemq ! body: %s',(json_str,), logger, logging.DEBUG)
    mqClient.send(body=json_str, destination='/queue/task_result')

def runcommands_by_popen(message):
    jretstr = runcommands(message,logger)
    response_result_to_mq(jretstr)

# activemq的消息listener
class ActivemqMsgListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_error(self, headers, message):
        vprint('activemq received an error "%s"' ,( message,), logger, logging.ERROR)

    def on_connected(self, headers, body):
        vprint('connect to activemq', None,logger, logging.INFO)

    def on_disconnected(self):
        vprint('disconnect to activemq', None,logger, logging.INFO)
        global UnhandledInterruptHappend
        if UnhandledInterruptHappend == False:
            time.sleep(RECONNECT_SLEEP_INTEVAL)
            vprint('reconnect to activemq', None,logger, logging.INFO)
            connect_and_subscribe(self.conn)
        else:
            pass

    def on_message(self, headers, message):
        vprint('received a message %s' , (message,), logger, logging.DEBUG)
        t = threading.Thread(target=runcommands_by_popen,args=(message,))
        t.start()

# rpyc服务
class RpycWorkerService(rpyc.Service):
    # 登陆
    def exposed_login(self,user,passwd):
        if user=="OMuser" and passwd=="KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h":
            self.Checkout_pass=True
        else:
            self.Checkout_pass=False

    # 执行任务
    def exposed_Runcommands(self, get_string):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")
        # 解密命令并调用
        jretstr = runcommands(tdecode(get_string, RPYC_SECRET_KEY), logger)
        return self.response("C_0000", jretstr)

    def exposed_QueryMemberInfo(self):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")
        #环境信息  
        infos={}
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
            vprint('QueryMemberInfo exception: %s' ,(str(e),), logger, logging.DEBUG)
        return self.response("C_0000", json.dumps(infos))

    def exposed_Shutdownit(self, workid):
        try:
            if self.Checkout_pass == True:
                dworkid = tdecode(workid,RPYC_SECRET_KEY)
                vprint('server is closed by : %s', (str(dworkid),), logger, logging.INFO)
                global rpycserver
                rpycserver.close()
                rpycserver = None
            else:
                pass
        except Exception, e:
            vprint('[%s]received a exception %s', ('RpycService Shutdown',str(e),), logger, logging.ERROR)

    def response(self,code, message):
        dict={}
        dict['code'] = str(code)
        dict['msg'] = str(message)
        dictstr = json.dumps(dict);

        vprint('response: %s' , (dictstr,), logger, logging.DEBUG)
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
        zkClient.ensure_path(ZOOKEEPER_PARENT_PATH + "/worker/worker")
        zkClient.ensure_path(ZOOKEEPER_PARENT_PATH + "/worker/worker/path")
        # 创建zk节点
        zkClient.create(ZOOKEEPER_PARENT_PATH + "/worker/worker/" + str(RPYC_HOST) + '_' + str(RPYC_PORT), value=b'', ephemeral=True)
        zkClient.create(ZOOKEEPER_PARENT_PATH + "/worker/worker/path/" + str(RPYC_HOST) + '_' + str(RPYC_PORT), value=sysdir.decode("GBK").encode("utf-8"), ephemeral=True)

        vprint('connect to activemq hosts:=%s!' ,ACTIVEMQ_HOSTS, logger, logging.INFO)
        mqClient = stomp.Connection(ACTIVEMQ_HOSTS_LISTS, heartbeats=(8000, 8000))
        mqClient.set_listener('ActivemqMsgSenderListener', ActivemqMsgListener(mqClient))
        connect_and_subscribe(mqClient)

        vprint('start rpyc connection thread! host:%s port:%s' , (str(RPYC_HOST),str(RPYC_PORT)), logger, logging.INFO)
        rpycserver=ThreadedServer(RpycWorkerService, port=int(RPYC_PORT),auto_register=False)
        rpycserver.start()
    except Exception, e:
        vprint('[%s]received a exception %s' ,('main',str(e),), logger, logging.ERROR)
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
            if rpycserver is not None:
                rpycserver.close()
        except:
            pass
        vprint('task %s is stop!' ,(str(workerid)), logger, logging.INFO)