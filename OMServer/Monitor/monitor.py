# -*- coding: UTF-8 -*-
#!/usr/bin/env python
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
# 系统版本
import platform
import subprocess
# 模块及配置
from config import *
from libraries import *

# 当前worker的id
workerid = uuid.uuid4() 
pid= os.getpid()
# 初始化变量
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', filename=sys.path[0]+'/logs/server_' + str(workerid) + '.log', filemode='a')
logger = logging.getLogger()
# zookeeper链接
zkClient = None
UnhandledInterruptHappend=False

reload(sys)
sys.setdefaultencoding('utf-8')
sysdir=os.path.abspath(os.path.dirname(__file__))

# 不指定启动机器及端口，则随机生成
if RPYC_LEADER_HOST == '':
    RPYC_LEADER_HOST = socket.gethostbyname(socket.gethostname())

if RPYC_LEADER_PORT == '':
    RPYC_LEADER_PORT = random_port(16000,17000, 10)

def none2str(value):
    if value is not None:
        return str(value)
    return ''

# 获取zk节点的值
def get_zknode_str_value(zkClient, vpath):
    try:
        data, stat = zkClient.get(str(vpath),watch=None)
        if data is not None:
            return data.decode('utf-8')
        return ''
    except Exception, e:
        vprint('[%s]received a exception %s %s', ('get_zknode_str_value',vpath, str(e),), logger, logging.ERROR)
    return None

# rpyc服务
class RpycManagerService(rpyc.Service):
    # 登陆
    def exposed_login(self,user,passwd):
        if user=="OMuser" and passwd=="KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h":
            self.Checkout_pass=True
        else:
            self.Checkout_pass=False

    # 查询worker节点信息
    def exposed_ServerInfos(self):
        try:
            if self.Checkout_pass!=True:
                return self.response("C_9010","User verify failed!")
        except:
            return self.response("C_9001", "Invalid Login!")
        infos={}
        lhostport = get_zknode_str_value(zkClient, str(MONITOR_PARENT_PATH + "/leader/leader"))
        if lhostport is not None and lhostport <> '':
            (lhost, lport) = lhostport.split("_")
            response = self.request_leader(lhost,lport)
            if response is not None:
                jresponse = json.loads(response)
                infos['leader'] = jresponse['msg']
        
        whostport = get_zknode_str_value(zkClient, str(MONITOR_PARENT_PATH + "/worker/leader"))
        if whostport is not None and whostport <> '':
            (whost, wport) = whostport.split("_")
            response = self.request_worker(whost,wport)
            if response is not None:
                jresponse = json.loads(response)
                infos['worker'] = jresponse['msg']

        infos['monitor']={}
        infos['monitor']['host'] = str(RPYC_LEADER_HOST)
        infos['monitor']['port'] = str(RPYC_LEADER_PORT)
        infos['monitor']['path'] = get_zknode_str_value(zkClient, str(MONITOR_PARENT_PATH + "/worker/leader/path"))
        #环境信息
        try:
            # 获取硬件信息
            import psutil
            global pid
            cprocess=psutil.Process(pid)
            infos['monitor']['memory']=str("%.2f M" % (psutil.virtual_memory().total/(1024*1024)))
            infos['monitor']['memory_percent']=str(psutil.virtual_memory().percent) + '%'
            infos['monitor']['pid_memory_percent']="%.2f%%" % (cprocess.memory_percent())
            infos['monitor']['cpu_percent']=str(psutil.cpu_percent(0.5)) + '%'
            infos['monitor']['pid_cpu_percent']=str(cprocess.cpu_percent()) + '%'
            infos['monitor']['pid']=str(os.getpid())
        except Exception, e:
            vprint('QueryWorks exception: %s' ,(str(e), ), logger, logging.DEBUG)
        return self.response("C_0000", json.dumps(infos))

    def request_leader(self, host, port):
        try:
            conn=rpyc.connect(host,int(port))
            conn.root.login('OMuser','KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h')
        except Exception,e:
            return None
        result = tdecode(conn.root.QuerySchedulers(),RPYC_SECRET_KEY)
        return result

    def request_worker(self, host, port):
        try:
            conn=rpyc.connect(host,int(port))
            conn.root.login('OMuser','KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h')
        except Exception,e:
            return None
        result = tdecode(conn.root.QueryWorks(),RPYC_SECRET_KEY)
        return result

    def response(self,code, message):
        dict={}
        dict['code'] = str(code)
        dict['msg'] = str(message)
        dictstr = json.dumps(dict);

        vprint('response: %s' , (dictstr,), logger, logging.DEBUG)
        return tencode(json.dumps(dict),RPYC_SECRET_KEY)

def ping_host(host, port):
        try:
            conn=rpyc.connect(host,int(port))
            result = tdecode(conn.root.pingit(),RPYC_SECRET_KEY)
            jreuslt = json.loads(result)
            if jreuslt['code'] == 'C_0000':
                return True
        except Exception,e:
            pass
        return False

def get_static_hosts_info(htype):
    dbconn = None
    dbcur = None

    jstrObj=[]
    try:
        dbconn = psycopg2.connect(database=DB_DATABASE, user=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        dbcur = dbconn.cursor()
        dbcur.execute("SELECT host,port,username,password FROM aom_task_hosts_static where htype=" + htype)
        rows = dbcur.fetchall()
        for row in rows:
            jstrDict = {}
            jstrDict['host'] = none2str(row[0])
            jstrDict['port'] = none2str(row[1])
            jstrDict['username'] = none2str(row[2])
            jstrDict['password'] = none2str(row[3])
            jstrObj.append(jstrDict)
        return jstrObj
    except Exception, e:
        vprint('[%s]received a exception %s' , ('get_static_hosts_info',str(e),), logger,logging.ERROR)
    finally:
        if dbcur is not None:
            dbcur.close()
        if dbconn is not None:
            dbconn.close()
    return None

def run_local_executable_file_as_daemon(cmd):
    try:
        #p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)  
        #p.wait()  
        os.system(cmd) 
    except Exception,e:
        vprint('[%s]received a exception %s',('run_local_executable_file_as_daemon',str(e),), logger,logging.ERROR)

def ssh_remote_and_restart(hostname, port, username, password, lpath):
    try:
        strcommand = ''
        if port == '' or port == '-':
            # 默认为本地
            if platform.system() == "Windows":
                strcommand='cmd.exe /c ' + lpath + '\startLeader.bat'
                strcommand= strcommand.decode('utf-8').encode('gbk')
            else:
                strcommand='/bin/sh -c ' + lpath + '/startLeader.sh'
            #设置线程为后台线程,Monitor被关闭，则也关闭
            t =threading.Thread(target=run_local_executable_file_as_daemon,args=(strcommand,))
            t.start()
            vprint('[ssh_remote_and_restart]start local script as daemon thread! %s' , (str(strcommand),), logger,logging.INFO)
        else:
            if password <> '':
                import paramiko
                npassword = tdecode(password,RPYC_SECRET_KEY)
                ssh = paramiko.SSHClient()
                ssh.load_system_host_keys()
                ssh.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy())
                ssh.connect(hostname, port=int(port), username=username, password=password)
                #默认远程机器为Linux
                strcommand='/bin/sh -c ' + lpath + '/startLeader.sh'
                stdin, stdout, stderr = ssh.exec_command(strcommand)
                ssh.close()
                vprint('[ssh_remote_and_restart]start remote script by ssh! %s' ,(str(strcommand),), logger,logging.INFO)
            else:
                pass
    except Exception,e:
        vprint('[%s]received a exception %s',('ssh_remote_and_restart',str(e),), logger,logging.ERROR)
    

def judge_thread_alive_or_not(zk_hp_path, zk_wk_host, htype):
    global zkClient
    lhostport = get_zknode_str_value(zkClient, zk_hp_path)
    
    thread_name = 'schedulers leader thread'
    if htype == "1":
        thread_name = 'task leader thread'
    else:
        pass

    if lhostport is not None or lpath <> '':
        (lhost, lport) = lhostport.split('_')
        if ping_host(lhost, lport) == False:
            vprint('%s is not start or all dead !' ,(thread_name,), logger, logging.INFO)
            lpath = get_zknode_str_value(zkClient, zk_wk_host)
            if lpath is not None and lpath <> '':
                shosts = get_static_hosts_info(htype)
                if shosts is not None:
                    bfound = False
                    for i in range(0, len(shosts)):
                        if shosts[i]['host'] == lhost:
                            # 尝试重启服务
                            bfound = True
                            vprint('location of %s has been found !',(thread_name,), logger, logging.INFO)
                            ssh_remote_and_restart(shosts[i]['host'], shosts[i]['port'], shosts[i]['username'], shosts[i]['password'], lpath)
                            break
                    if bfound == False:
                        vprint('skip to restart %s, cause cant find connection infos !',(thread_name,), logger, logging.INFO)
                else:
                    vprint('cant find static host for %s !', (thread_name,), logger, logging.INFO)
            else:
                pass
        else:
            pass

# 监视线程
def worker_monitor(args):
    while UnhandledInterruptHappend <> True:
        try:
            judge_thread_alive_or_not(str(MONITOR_PARENT_PATH + "/leader/leader"),str(MONITOR_PARENT_PATH + "/leader/leader/path"), "0")
            judge_thread_alive_or_not(str(MONITOR_PARENT_PATH + "/worker/leader"),str(MONITOR_PARENT_PATH + "/worker/leader/path"), "1")

            time.sleep(MONITOR_LEADER_INTEVAL)
        except Exception, e:
            vprint('[%s]received a exception %s', ('worker_monitor',str(e),), logger, logging.ERROR)
            break
    vprint('worker_monitor thread is stop!', None, logger, logging.INFO)

# 选举成为leader线程后，执行函数
def taskdispacher_leader_func():
    vprint('task %s became leader!' , (str(workerid),), logger, logging.INFO)
    try:
        vprint('start observation thread for followed worker!', None, logger, logging.INFO)
        watherthread = threading.Thread(target=worker_monitor,args=(None,))
        watherthread.start()

        vprint('start rpyc connection thread! %s %s' ,(str(RPYC_LEADER_HOST),str(RPYC_LEADER_PORT),), logger, logging.INFO)
        rpycserver=ThreadedServer(RpycManagerService,port=RPYC_LEADER_PORT,auto_register=False)
        rpycserver.start()
    except Exception, e:
        vprint('[%s]received a exception %s', ('taskdispacher_leader_func',str(e),), logger, logging.ERROR)

def set_interrupt_happend():
    global UnhandledInterruptHappend
    UnhandledInterruptHappend = True

if __name__ == '__main__':
    try:
        vprint('task %s is start!', (str(workerid),), logger, logging.INFO)
        vprint('connect to zookeeper host:=%s port:=%s!',(ZOOKEEPER_HOST,ZOOKEEPER_PORT,), logger, logging.INFO)
        # 连接zookeeper
        zkClient = KazooClient(hosts=ZOOKEEPER_HOST + ':' + ZOOKEEPER_PORT)
        zkClient.start()
        if not zkClient.exists(MONITOR_PARENT_PATH + "/monitor/electionpath"):
            # 确认路径，如果有必要则创建该路径
            zkClient.ensure_path(MONITOR_PARENT_PATH + "/monitor/electionpath")
        vprint('waiting for the election!', None, logger, logging.INFO)
        # 阻塞线程，直到选举成功。并调用taskdispacher_leader_func
        election = zkClient.Election(MONITOR_PARENT_PATH + "/monitor/electionpath")
        election.run(taskdispacher_leader_func)
    except Exception, e:
        set_interrupt_happend()
        vprint('[%s]received a exception %s',('main',str(e),), logger, logging.ERROR)
    finally:
        if zkClient is not None:
            zkClient.stop()
        vprint('task %s is stop!', (str(workerid),), logger, logging.INFO)