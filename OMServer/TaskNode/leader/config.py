# -*- coding: utf-8 -*-
#!/usr/bin/env python

STATIC_CONFIGS = {
    'WORK_ID':'28539fcc-b674-4ba2-8651-fb7ecfab11b8-001',
    'WATCHER':{
        'SLEEP_INTEVAL':60,
        'BALANCE':{
            'AUTO':True
        },
    },
    'ZOOKEEPERS': {
        'HOSTS':'127.0.0.1:2181',
        'START_PATH':'/test',
    },
    'ACTIVEMQ': {
        'HOSTS':'127.0.0.1:61613',
        'USER':'',
        'PASSWORD':'',
        'RECONNECT_SLEEP_INTEVAL':10,
    },
    'DATABASES':{
        'NAME': 'aom',
        'USER':'postgres',
        'PASSWORD':'postgres',
        'HOST':'127.0.0.1',
        'PORT':'5432',
    },
    'RPYCS':{
        'SECRET_KEY':'ctmj#&amp;8hrgow_^sj$ejt@9fzsmh_o)-=(byt5jmg=e3#foya6u',
        'HOST':'127.0.0.1',
        'PORT':'11511',
        'WEIGHT':'1',
    },
    'LOGS':{
        'CONSOLE_PRINT':True,
        'LEVEL':'DEBUG',
    },
}

