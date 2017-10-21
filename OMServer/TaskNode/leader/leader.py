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
# 定时任务
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
# 通讯rpyc模块
import rpyc 
from rpyc.utils.server import ThreadedServer

# 模块及配置
from config import *
from modules.modules import *
from libraries import *

# 当前worker的id
workerid = uuid.uuid4()
pid= os.getpid()

# 初始化变量
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', filename=sys.path[0]+'/logs/server_' + str(workerid) + '.log', filemode='a')
logger = logging.getLogger()

# 初始化定时器
scheduler = BackgroundScheduler()
scheduler.start()
# activemq链接
mqClient = None
# zookeeper链接
zkClient = None
gworkers={}
gschedulerList=[]
gschedulerListCount=0

UnhandledInterruptHappend=False
reload(sys)
sys.setdefaultencoding('utf-8')
sysdir=os.path.abspath(os.path.dirname(__file__))

# 不指定启动机器及端口，则随机生成
if RPYC_LEADER_HOST == '':
    RPYC_LEADER_HOST = socket.gethostbyname(socket.gethostname())

if RPYC_LEADER_PORT == '':
    RPYC_LEADER_PORT = random_port(17000,18000, 10)

# 链接activemq
def connect_and_subscribe(conn):
    conn.start()
    if not ACTIVEMQ_USER and ACTIVEMQ_USER <> '':
        vprint('connect activemq with auth!', None, logger, logging.INFO)
        conn.connect(ACTIVEMQ_USER, ACTIVEMQ_USER, wait=True)
    else:
        conn.connect()

# activemq的消息listener
class ActivemqMsgListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_error(self, headers, message):
        vprint('activemq received an error "%s"' ,(message,), logger, logging.ERROR)

    def on_connected(self, headers, body):
        vprint('connect to activemq', None, logger, logging.INFO)

    def on_disconnected(self):
        vprint('disconnect to activemq', None, logger, logging.INFO)
        global UnhandledInterruptHappend
        if UnhandledInterruptHappend == False:
            time.sleep(30)
            vprint('reconnect to activemq', None,logger, logging.INFO)
            connect_and_subscribe(self.conn)
        else:
            pass

# 定时触发，提交任务到mq
def execute_task_submit(json_str):
    global mqClient
    vprint('submit task to activemq ! body: %s',(json_str,), logger, logging.DEBUG)
    mqClient.send(body=json_str, destination='/queue/task')

def none2str(value):
    if value is not None:
        return str(value)
    return ''

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

def vadd_job_message(message):
    try:
        jobject = json.loads(message)
        # 获取cron表达式
        jcron = json.loads(jobject[0]['module_cron'])
        vadd_job(jcron, message)
    except Exception, e:
        vprint('[%s]received a exception %s' ,('vadd_job_message', message + str(e)), logger, logging.ERROR)

# 重新加载定时任务
def get_scheduler_task():
    global workerid
    dbconn = None
    dbcur = None

    jschedulerList=[]
    try:
        dbconn = psycopg2.connect(database=DB_DATABASE, user=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        dbcur = dbconn.cursor()
        dbcur.execute("SELECT id,parentid,name,module_name,module_oper,module_cron,module_order FROM aom_task_schedule order by id, parentid, module_order asc;")
        rows = dbcur.fetchall()

        ptaskpid=''
        ctaskpid=''
        jstrObj=[]

        vprint('the number of sechedule task after query is %s' ,(str(len(rows))), logger,logging.INFO)
        for row in rows:
            ctaskpid = row[1]
            if ptaskpid <> ctaskpid:
                if len(jstrObj) > 0:
                    jschedulerList.append(json.dumps(jstrObj))
                else:
                    pass
                del jstrObj[:]
                ptaskpid = ctaskpid
            else:
                pass
            jstrDict = {}
            jstrDict['id'] = none2str(row[0])
            jstrDict['parentid'] = none2str(row[1])
            jstrDict['name'] = none2str(row[2])
            jstrDict['module_name'] = none2str(row[3])
            jstrDict['module_oper'] = none2str(row[4])
            jstrDict['module_cron'] = none2str(row[5])
            jstrDict['module_order'] = none2str(row[6])
            jstrDict['from'] = str(workerid)
            jstrObj.append(jstrDict)

        jschedulerList.append(json.dumps(jstrObj))
        vprint('the number of merged sechedule task is %s' ,(str(len(jschedulerList))), logger,logging.INFO)
    except Exception, e:
        vprint('[%s]received a exception %s' ,('get_scheduler_task', str(e)), logger, logging.ERROR)
    finally:
        if dbcur is not None:
            dbcur.close()
        if dbconn is not None:
            dbconn.close()
    return jschedulerList

# 通知worker刷新定时任务
def transfer_schedulertask_toworker(hostport, lschedulerList, startp, endp):
    try:
        (host, port) = hostport.split('_')
        rpycconn=rpyc.connect(host,int(port))
        rpycconn.root.login('OMuser','KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h')
        vprint('sync scheduler task [host:=%s port:=%s]' , (host, port), logger, logging.INFO)
        result=tdecode_rpyc_res(rpycconn.root.SyncTaskerStart(),RPYC_SECRET_KEY)
        if result['code'] == 'C_0000':
            for i in range(startp, endp):
                presult=tdecode_rpyc_res(rpycconn.root.SyncTaskerInfo(tencode(lschedulerList[i],RPYC_SECRET_KEY)),RPYC_SECRET_KEY)
                if presult['code'] <> 'C_0000':
                    vprint('sync scheduler task [host:=%s port:=%s] failure! request: %s' ,(host, port, lschedulerList[i]), logger, logging.WARN)
                    vprint('sync scheduler task [host:=%s port:=%s] failure! response: %s', (host, port, presult['msg']), logger, logging.WARN)
                else:
                    vprint('sync scheduler task [host:=%s port:=%s] sucess! request: %s' ,(host, port, lschedulerList[i]), logger, logging.DEBUG)

            result=tdecode_rpyc_res(rpycconn.root.SyncTaskerComplete(),RPYC_SECRET_KEY)
            if result['code'] == 'C_0000':
                vprint('sync scheduler task complete ! [host:=%s port:=%s] count:=%s' , (host, port, str(endp - startp)), logger, logging.INFO)
        else:
            pass
    except Exception,e:
        vprint('[transfer_schedulertask_toworker]connect rpyc server error: %s %s',(hostport,str(e)), logger, logging.ERROR)

# 获取worker的任务数
def get_schedulerwroker_infos(hostport):
    try:
        (host, port) = hostport.split('_')
        rpycconn=rpyc.connect(host,int(port))
        rpycconn.root.login('OMuser','KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h')
        result=tdecode_rpyc_res(rpycconn.root.QueryMemberInfo(),RPYC_SECRET_KEY)
        if result['code'] == 'C_0000':
            return json.loads(result['msg'])
        return ''
    except Exception,e:
        vprint('[get_schedulerwroker_infos]connect rpyc server error: %s %s',(hostport,str(e)), logger, logging.ERROR)

# 简单均衡分布策略
def sample_blance_worker_task_count(lworkkeys, total, countSum, countRecords):
    # 简单的平衡补差
    counti=0
    if countSum > total:
        countMinus = countSum - total
        for k,v in lworkkeys:
            if counti < countMinus:
                if countRecords.has_key(k):
                    counti+=1
                    countRecords[k] = countRecords[k] - 1
                else:
                    pass
            else:
                break

    elif countSum < total:
        countMinus = total - countSum
        for k,v in lworkkeys:
            if counti < countMinus:
                if countRecords.has_key(k):
                    counti+=1
                    countRecords[k] = countRecords[k] + 1
                else:
                    pass
            else:
                break

# 计算各个worker上的任务数
def get_worker_task_count(lwork, total):
    # 计算总数
    weightSum=0
    for k,v in lwork.items():
        if lwork[k]['weight'] <> '':
            weightSum+=int(lwork[k]['weight'])
    countRecords={}
    for k,v in lwork.items():
        if lwork[k]['weight'] <> '':
            countRecords[k]=round((int(lwork[k]['weight'])/weightSum)*total)
            #print 'weightSum2:' + str(int(lwork[k]['weight'])) + ' ' + str((int(lwork[k]['weight'])/weightSum)*total)
        else :
            pass
    countSum=0
    for k,v in lwork.items():
        countSum+=countRecords[k]
    sample_blance_worker_task_count(lwork.items(), total, countSum, countRecords)
    return countRecords

# 多线程同步定时任务列表到worker上
def reload_scheduler_task(lwork):
    global scheduler
    global gschedulerList
    global gschedulerListCount
    lschedulerList = get_scheduler_task()
    gschedulerListCount = len(lschedulerList)
    if len(lwork) == 0:
        if cmpare_dict(gschedulerList, lschedulerList) == False:
            vprint('[reload_scheduler_task]the leader task is attempt to scheduler task!', None, logger, logging.INFO)
            del gschedulerList[:]
            gschedulerList = lschedulerList[:]
            vprint('[reload_scheduler_task]the task are changed!stop scheduler and remove older jobs!', None,logger,logging.INFO)

            if scheduler.running == True:
                scheduler.pause()
                scheduler.remove_all_jobs()
            vprint('[reload_scheduler_task]submit new job into scheduler!',None, logger,logging.INFO)
            for i in range(0, len(gschedulerList)):
                vadd_job_message(gschedulerList[i])

            scheduler.resume()
        else:
            pass

    else:
        countRecords = get_worker_task_count(lwork, len(lschedulerList))
        # 同步到各个worker上
        wthreads=[]
        startp = 0
        endp =0
        for k,v in lwork.items():
            if countRecords.has_key(k):
                startp = endp
                endp = startp + int(countRecords[k])
                #print str(startp) + ' ' + str(endp) + ' ' + str(countRecords[k])
                t = threading.Thread(target=transfer_schedulertask_toworker,args=(k,lschedulerList,startp,endp))
                wthreads.append(t)
            else:
                pass
        for t in wthreads:  
            t.start();

# 获取zk节点的值
def get_zknode_int_value(zkClient, vpath):
    try:
        data, stat = zkClient.get(str(vpath),watch=None)
        if data is not None:
            if data == '':
                return 1
            return int(data)
        return 1
    except Exception, e:
        vprint('[%s]received a exception %s %s' ,('get_zknode_int_value',vpath, str(e)), logger, logging.ERROR)
    return None

# 获取zk节点的值
def get_zknode_str_value(zkClient, vpath):
    try:
        data, stat = zkClient.get(str(vpath),watch=None)
        if data is not None:
            return data.decode('utf-8')
        return ''
    except Exception, e:
        vprint('[%s]received a exception %s %s' , ('get_zknode_str_value',vpath, str(e),), logger, logging.ERROR)
    return None

# 获取worker节点及对应的负载率
def get_worklist_nodevalue(bGetTaskCount):
    global zkClient
    lwork={}
    if zkClient.exists(MONITOR_PARENT_PATH + "/leader/worker"):
        children = zkClient.get_children(MONITOR_PARENT_PATH + "/leader/worker")
        if len(children) == 0:
            #worker全部消亡
            vprint('no works or workers are all dead!', None, logger, logging.WARN)
        else:
            for child in children:
                if not lwork.has_key(child) and child <> 'path':
                    vchild = get_zknode_int_value(zkClient, str(MONITOR_PARENT_PATH + "/leader/worker/" + child))
                    if vchild is not None:
                        lwork[child]={}
                        (lhost,lport) = child.split('_')
                        lwork[child]['host'] = lhost
                        lwork[child]['port'] = lport
                        lwork[child]['weight'] = vchild
                        lwork[child]['path'] = get_zknode_str_value(zkClient, str(MONITOR_PARENT_PATH + "/leader/worker/path/" + child))
                        if bGetTaskCount == True:
                            infos = get_schedulerwroker_infos(child)
                            lwork[child]['count'] = infos['count']
                            lwork[child]['memory'] = infos['memory']
                            lwork[child]['memory_percent'] = infos['memory_percent']
                            lwork[child]['pid_memory_percent'] = infos['pid_memory_percent']
                            lwork[child]['cpu_percent'] = infos['cpu_percent']
                            lwork[child]['pid_cpu_percent'] = infos['pid_cpu_percent']
                            lwork[child]['pid'] = infos['pid']
                            
    else:
        # 没有worker启动
        vprint('no works!', None, logger, logging.INFO)
    return lwork

# 更新worker节点的负载率
def set_worklist_nodevalue(jobject):
    global zkClient
    for k,v in jobject.items():
        if zkClient.exists(MONITOR_PARENT_PATH + "/leader/worker/" + k):
            zkClient.set(MONITOR_PARENT_PATH + "/leader/worker/" + k, str(v))

# rpyc服务
class RpycManagerService(rpyc.Service):
    # 登陆
    def exposed_login(self,user,passwd):
        if user=="OMuser" and passwd=="KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h":
            self.Checkout_pass=True
        else:
            self.Checkout_pass=False

    def exposed_RebanlanceSchedulers(self):
        return self.exposed_SyncTasksToSchedulers()

    # 定时任务同步
    def exposed_SyncTasksToSchedulers(self):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")

        vprint('SyncTasksToSchedulers is invoked!', None, logger, logging.DEBUG)
        global gworkers
        lworkers = get_worklist_nodevalue(False)
        if cmpare_dict(gworkers, lworkers) == False:
            gworkers.clear()
            gworkers = lworkers.copy()
            vprint('followed workers are changed, try to balance task to followed workers', None, logger, logging.INFO)
            reload_scheduler_task(gworkers)

        return self.response("C_0000", "sucess")

    # 查询worker节点信息
    def exposed_QuerySchedulers(self):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")

        vprint('QuerySchedulers is invoked!', None, logger, logging.DEBUG)
        global gschedulerListCount
        lworkers = get_worklist_nodevalue(True)
        lworkers['leader']={}
        lworkers['leader']['count']=str(gschedulerListCount)
        lworkers['leader']['host'] = str(RPYC_LEADER_HOST)
        lworkers['leader']['port'] = str(RPYC_LEADER_PORT)
        lworkers['leader']['path'] = get_zknode_str_value(zkClient, str(MONITOR_PARENT_PATH + "/leader/leader/path"))
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
            vprint('QuerySchedulers exception: %s', (str(e),), logger, logging.DEBUG)
        return self.response("C_0000", json.dumps(lworkers))

    # 设置worker节点负载率
    def exposed_ResetSchedulersLoaderrate(self, message):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")

        vprint('ResetSchedulersLoaderrate is invoked!',None, logger, logging.DEBUG)
        try:
            dmessage = tdecode(message,RPYC_SECRET_KEY)
            jobject = json.loads(dmessage)
            set_worklist_nodevalue(jobject)
        except Exception, e:
            vprint('[%s]received a exception %s',('ResetSchedulersLoaderrate',str(e),), logger, logging.ERROR)
            return self.response("C_9098", str(e))

        return self.response("C_0000", "sucess")

    def exposed_pingit(self):
        return self.response("C_0000", "sucess")

    def response(self,code, message):
        dict={}
        dict['code'] = str(code)
        dict['msg'] = str(message)
        dictstr = json.dumps(dict);

        vprint('response: %s' ,(dictstr,), logger, logging.DEBUG)
        return tencode(json.dumps(dict),RPYC_SECRET_KEY)

# 监视followed worker的线程
def worker_monitor(args):
    global gworkers
    while UnhandledInterruptHappend <> True:
        lworkers = get_worklist_nodevalue(False)
        if len(lworkers) == 0:
            gworkers.clear()
            reload_scheduler_task(gworkers)
        else:
            if cmpare_dict(gworkers, lworkers) == False:
                # 如果followed worker有变动(个数、或ip或端口)
                gworkers.clear()
                gworkers = lworkers.copy()
                vprint('followed workers are changed!', None, logger, logging.INFO)
                if MONITOR_LEADER_AUTO_BALANCE == True:
                    vprint('blance task to followed workers', None, logger, logging.INFO)
                    reload_scheduler_task(gworkers)
                else:
                    pass
        time.sleep(MONITOR_LEADER_INTEVAL)
    vprint('observation thread is stop!',None, logger, logging.INFO)

# 选举成为leader线程后，执行函数
def taskdispacher_leader_func():
    vprint('task %s became leader!', (str(workerid),), logger, logging.INFO)
    global mqClient
    try:
        # 更新zknode
        zkClient.ensure_path(MONITOR_PARENT_PATH + "/leader/leader")
        zkClient.set(MONITOR_PARENT_PATH + "/leader/leader", str(RPYC_LEADER_HOST) + '_' + str(RPYC_LEADER_PORT))
        zkClient.ensure_path(MONITOR_PARENT_PATH + "/leader/leader/path")
        zkClient.set(MONITOR_PARENT_PATH + "/leader/leader/path", sysdir.decode("GBK").encode("utf-8"))

        #连接代码分协议版本
        vprint('connect to activemq host:=%s port:=%s!' ,(ACTIVEMQ_HOST,ACTIVEMQ_PORT), logger, logging.INFO)
        mqClient = stomp.Connection([(ACTIVEMQ_HOST,ACTIVEMQ_PORT)], heartbeats=(8000, 8000))
        mqClient.set_listener('ActivemqMsgSenderListener', ActivemqMsgListener(mqClient))
        connect_and_subscribe(mqClient)

        vprint('start observation thread for followed worker!',None, logger, logging.INFO)
        watherthread = threading.Thread(target=worker_monitor,args=(None,))
        watherthread.start()

        vprint('start rpyc connection thread! host:%s port:%s' , (str(RPYC_LEADER_HOST),str(RPYC_LEADER_PORT)), logger, logging.INFO)
        rpycserver=ThreadedServer(RpycManagerService,port=RPYC_LEADER_PORT,auto_register=False)
        rpycserver.start()
    except Exception, e:
        vprint('[%s]received a exception %s' , ('taskdispacher_leader_func',str(e),), logger, logging.ERROR)
    finally:
        if mqClient is not None:
            mqClient.disconnect()

def set_interrupt_happend():
    global UnhandledInterruptHappend
    UnhandledInterruptHappend = True

if __name__ == '__main__':
    try:
        vprint('task %s is start!' , (str(workerid),), logger, logging.INFO)
        vprint('connect to zookeeper host:=%s port:=%s!' , (ZOOKEEPER_HOST,ZOOKEEPER_PORT,), logger, logging.INFO)
        # 连接zookeeper
        zkClient = KazooClient(hosts=ZOOKEEPER_HOST + ':' + ZOOKEEPER_PORT)
        zkClient.start()
        if not zkClient.exists(MONITOR_PARENT_PATH + "/leader/electionpath"):
            # 确认路径，如果有必要则创建该路径
            zkClient.ensure_path(MONITOR_PARENT_PATH + "/leader/electionpath")
        vprint('waiting for the election!', None, logger, logging.INFO)
        # 阻塞线程，直到选举成功。并调用taskdispacher_leader_func
        election = zkClient.Election(MONITOR_PARENT_PATH + "/leader/electionpath")
        election.run(taskdispacher_leader_func)
    except Exception, e:
        set_interrupt_happend()
        vprint('[%s]received a exception %s', ('main',str(e),), logger, logging.ERROR)
    finally:
        if zkClient is not None:
            zkClient.stop()
        if scheduler is not None:
            scheduler.shutdown()
        vprint('task %s is stop!' , (str(workerid),), logger, logging.INFO)