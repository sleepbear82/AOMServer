# -*- coding: utf-8 -*-
#from django.http import HttpResponse
from django.shortcuts import render
from django.db import connection
from django.conf import settings
from django.http import HttpResponse

import logging
logger = logging.getLogger('django')

import rpyc

from public.views import *

def index(request):
    context={'system_name': settings.SYSTEM_NAME}
    return render(request, 'stats/main.html', context)

def scheduler_load(request):
    try:
        conn=rpyc.connect('127.0.0.1',11513)
        conn.root.login('OMuser','KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h')
    except Exception,e:
        logger.error('connect rpyc server error:'+str(e))
        return HttpResponse('connect rpyc server error:'+str(e))

    OPresult=tdecode(conn.root.ServerInfos(),settings.SECRET_KEY)
    return HttpResponse(OPresult)

def scheduler_rebalance(request):
    try:
        conn=rpyc.connect('127.0.0.1',11513)
        conn.root.login('OMuser','KJS23o4ij09gHF734iuhsdfhkGYSihoiwhj38u4h')
    except Exception,e:
        logger.error('connect rpyc server error:'+str(e))
        return HttpResponse('connect rpyc server error:'+str(e))

    OPresult=tdecode(conn.root.RebanlanceSchedulers(),settings.SECRET_KEY)
    return HttpResponse(OPresult)