# -*- coding: utf-8 -*-
from Public_lib import *

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
#查看系统日志模块#

class Modulehandle():
    def __init__(self,moduleid,hosts,sys_param_row):
        self.hosts = ""
        self.Runresult = ""
        self.moduleid = moduleid
        self.sys_param_array= sys_param_row
        self.hosts=target_host(hosts,"IP")

    def run(self):
        return "简单的回复问题！"
