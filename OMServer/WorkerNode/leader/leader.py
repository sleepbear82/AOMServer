# -*- coding: UTF-8 -*-
# 基本
from __future__  import division
import time
import uuid
import logging
import sys
import json
import threading
import os
# 访问activemq
import stomp
# 访问zookeeper
from kazoo.client import KazooClient
# 访问数据库
import psycopg2
# 通讯rpyc模块
import rpyc 
from rpyc.utils.server import ThreadedServer

# 模块及配置
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
if STATIC_CONFIGS['RPYCS'].has_key('HOST') == True:
    RPYC_HOST=STATIC_CONFIGS['RPYCS']['HOST']

if STATIC_CONFIGS['RPYCS'].has_key('PORT') == True:
    RPYC_PORT=STATIC_CONFIGS['RPYCS']['PORT']

# 不指定启动机器及端口，则随机生成
if RPYC_HOST == '':
    RPYC_HOST = socket.gethostbyname(socket.gethostname())

if RPYC_PORT == '':
    RPYC_PORT = random_port(18000,19000, 10)

ZOOKEEPER_HOSTS='127.0.0.1:2181'
ZOOKEEPER_PARENT_PATH='/test'
if STATIC_CONFIGS.has_key('ZOOKEEPERS') == True:
    if STATIC_CONFIGS['ZOOKEEPERS'].has_key('HOSTS') == True:
        ZOOKEEPER_HOSTS = STATIC_CONFIGS['ZOOKEEPERS']['HOSTS']
    if STATIC_CONFIGS['ZOOKEEPERS'].has_key('START_PATH') == True:
        ZOOKEEPER_PARENT_PATH = STATIC_CONFIGS['ZOOKEEPERS']['START_PATH']

WATCHER_SLEEP_INTEVAL=60
if STATIC_CONFIGS.has_key('WATCHER') == True:
    if STATIC_CONFIGS['WATCHER'].has_key('SLEEP_INTEVAL') == True:
        WATCHER_SLEEP_INTEVAL=STATIC_CONFIGS['WATCHER']['SLEEP_INTEVAL']

# 当前pid
pid= os.getpid()
# 初始化日志
logging.basicConfig(level=logLevel, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', filename=sys.path[0]+'/logs/server_' + str(workerid) + '.log', filemode='a')
logger = logging.getLogger()
# zookeeper链接
zkClient = None
# rpycserver链接
rpycserver = None

UnhandledInterruptHappend=False
# default-encoding
reload(sys)
sys.setdefaultencoding('utf-8')
sysdir=os.path.abspath(os.path.dirname(__file__))

def none2str(value):
    if value is not None:
        return str(value)
    return ''

# 获取zk节点的值
def get_zknode_str_value(zkClient, vpath):
    try:
        if zkClient.exists(vpath):
            data, stat = zkClient.get(str(vpath),watch=None)
            if data is not None and data <> '':
                return data.decode('utf-8')
    except Exception, e:
        vprint('[%s]received a exception %s %s' ,('get_zknode_str_value',vpath, str(e)), logger, logging.ERROR)
    return None

# 获取worker的任务数
def get_wroker_infos(hostport):
    try:
        (host, port) = hostport.split('_')
        rpycconn=rpyc.connect(host,int(port))
        rpycconn.root.login('OMuser','KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h')
        result=tdecode_rpyc_res(rpycconn.root.QueryMemberInfo(),RPYC_SECRET_KEY)
        if result['code'] == 'C_0000':
            return json.loads(result['msg'])
        return None
    except Exception,e:
        vprint('[get_wroker_infos]connect rpyc server error: %s %s', (hostport,str(e)), logger, logging.ERROR)

# 获取worker节点列表
def get_worklist_node():
    global zkClient
    lwork={}
    if zkClient.exists(ZOOKEEPER_PARENT_PATH + "/worker/worker"):
        children = zkClient.get_children(ZOOKEEPER_PARENT_PATH + "/worker/worker")
        if len(children) == 0:
            #worker全部消亡
            vprint('no works or workers are all dead!',None, logger, logging.INFO)
        else:
            for child in children:
                if not lwork.has_key(child) and child <> 'path':
                    lwork[child]={}
                    (lhost,lport) = child.split('_')
                    lwork[child]['host'] = lhost
                    lwork[child]['port'] = lport
                    lwork[child]['path'] = get_zknode_str_value(zkClient, str(ZOOKEEPER_PARENT_PATH + "/worker/worker/path/" + child))
                    infos = get_wroker_infos(child)
                    if infos is not None:
                        lwork[child]['memory'] = infos['memory']
                        lwork[child]['memory_percent'] = infos['memory_percent']
                        lwork[child]['pid_memory_percent'] = infos['pid_memory_percent']
                        lwork[child]['cpu_percent'] = infos['cpu_percent']
                        lwork[child]['pid_cpu_percent'] = infos['pid_cpu_percent']
                        lwork[child]['pid'] = infos['pid']
                    else:
                        lwork[child]['memory'] = ''
                        lwork[child]['memory_percent'] = ''
                        lwork[child]['pid_memory_percent'] = ''
                        lwork[child]['cpu_percent'] = ''
                        lwork[child]['pid_cpu_percent'] = ''
                        lwork[child]['pid'] = ''
    else:
        # 没有worker启动
        vprint('no works!', None,logger, logging.INFO)
    return lwork

# rpyc服务
class RpycManagerService(rpyc.Service):
    # 登陆
    def exposed_login(self,user,passwd):
        if user=="OMuser" and passwd=="KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h":
            self.Checkout_pass=True
        else:
            self.Checkout_pass=False

    # 查询worker节点信息
    def exposed_QueryWorks(self):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")

        lworkers = get_worklist_node()
        lworkers['leader']={}
        lworkers['leader']['host'] = str(RPYC_HOST)
        lworkers['leader']['port'] = str(RPYC_LEADER_PORT)
        lworkers['leader']['path'] = get_zknode_str_value(zkClient, str(ZOOKEEPER_PARENT_PATH + "/worker/leader/path"))
        #环境信息
        try:
            # 获取硬件信息
            import psutil
            global pid
            cprocess=psutil.Process(pid)
            lworkers['leader']['memory']=str("%.2f M" % (psutil.virtual_memory().total/(1024*1024)))
            lworkers['leader']['memory_percent']=str(psutil.virtual_memory().percent) + '%'
            lworkers['leader']['pid_memory_percent']="%.2f%%" % (cprocess.memory_percent())
            lworkers['leader']['cpu_percent']=str(psutil.cpu_percent(0.5)) + '%'
            lworkers['leader']['pid_cpu_percent']=str(cprocess.cpu_percent()) + '%'
            lworkers['leader']['pid']=str(os.getpid())
        except Exception, e:
            vprint('QueryWorks exception: %s' ,(str(e),), logger, logging.DEBUG)
        return self.response("C_0000", json.dumps(lworkers))

    # 执行任务
    def exposed_Runcommands(self, get_string):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")
        # 解密命令并调用
        jretstr = runcommands(tdecode(get_string, RPYC_SECRET_KEY),logger)
        return self.response("C_0000", jretstr)

    def exposed_pingit(self):
        return self.response("C_0000", "sucess")

    def response(self,code, message):
        dict={}
        dict['code'] = str(code)
        dict['msg'] = str(message)
        dictstr = json.dumps(dict);

        vprint('response: %s' ,(dictstr,),logger, logging.DEBUG)
        return tencode(json.dumps(dict),RPYC_SECRET_KEY)

# 监视followed worker的线程
def worker_monitor(args):
    while UnhandledInterruptHappend <> True:
        lworkers = get_worklist_node()
        if len(lworkers) == 0:
            # TODO
            vprint('no works or workers are all dead!', None,logger, logging.WARN)
        else:
            pass
        time.sleep(WATCHER_SLEEP_INTEVAL)
    vprint('observation thread is stop!', None,logger, logging.INFO)

# 选举成为leader线程后，执行函数
def taskdispacher_leader_func():
    vprint('task %s became leader!' , (str(workerid),), logger, logging.INFO)
    try:
        # 更新zknode
        zkClient.ensure_path(ZOOKEEPER_PARENT_PATH + "/worker/leader")
        zkClient.set(ZOOKEEPER_PARENT_PATH + "/worker/leader", str(RPYC_HOST) + '_' + str(RPYC_PORT))
        zkClient.ensure_path(ZOOKEEPER_PARENT_PATH + "/worker/leader/path")
        zkClient.set(ZOOKEEPER_PARENT_PATH + "/worker/leader/path", sysdir.decode("GBK").encode("utf-8"))

        vprint('start observation thread for followed worker!', None,logger, logging.INFO)
        watherthread = threading.Thread(target=worker_monitor,args=(None,))
        watherthread.start()

        global rpycserver
        vprint('start rpyc connection thread! %s %s' ,(str(RPYC_HOST),str(RPYC_PORT)), logger, logging.INFO)
        rpycserver=ThreadedServer(RpycManagerService,port=int(RPYC_PORT),auto_register=False)
        rpycserver.start()
    except Exception, e:
        vprint('[%s]received a exception %s' ,('taskdispacher_leader_func',str(e)), logger, logging.ERROR)

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
        if not zkClient.exists(ZOOKEEPER_PARENT_PATH + "/worker/electionpath"):
            # 确认路径，如果有必要则创建该路径
            zkClient.ensure_path(ZOOKEEPER_PARENT_PATH + "/worker/electionpath")
        vprint('waiting for the election!', None,logger, logging.INFO)
        # 阻塞线程，直到选举成功。并调用taskdispacher_leader_func
        election = zkClient.Election(ZOOKEEPER_PARENT_PATH + "/worker/electionpath")
        election.run(taskdispacher_leader_func)
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
            if rpycserver is not None:
                rpycserver.close()
        except:
            pass
        vprint('task %s is stop!' ,(str(workerid),), logger, logging.INFO)