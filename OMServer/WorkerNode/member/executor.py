# -*- coding: UTF-8 -*-
import sys
import os
import json

from libraries import *

sysdir=os.path.abspath(os.path.dirname(__file__))

if __name__ == '__main__':
    try:
        message = sys.argv[1]
        message = tdecode(message, RPYC_SECRET_KEY)
        jmessage = json.loads(message)
        moduleid = jmessage['moduleid']
        mid="Mid_"+moduleid
        host = jmessage['host']
        param = jmessage['param']
        platformstr = 'local'
        if jmessage.has_key('platform'):
            platformstr = jmessage['platform']
        sys.path.append(os.sep.join((unicode(sysdir,'GB2312'),'modules/'+platformstr)))

        importstring = "from "+mid+" import Modulehandle"
        try:
            exec importstring
            runobj=Modulehandle(moduleid,host,param)
            runmessages=runobj.run()
            runmessages = runmessages.decode('utf-8').strip()
            if type(runmessages) == dict:
                #TODO 组装返回的string
                returnString = runmessages
            else:
                returnString = runmessages
            print tencode(returnString.encode('utf-8'), RPYC_SECRET_KEY)
        except Exception, e:
            print tencode(u"Module \""+mid+u"\" does not exist, Please add it" + e.message,RPYC_SECRET_KEY)
            #print "Module \""+mid+u"\" does not exist, Please add it" + str(e)
    except Exception, e:
        print tencode(u"executor received a exception:" + e.message,RPYC_SECRET_KEY)
        #print "executor received a exception:" + str(e)