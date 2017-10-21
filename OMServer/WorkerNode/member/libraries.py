# -*- coding: utf-8 -*-
#!/usr/bin/env python

import random, base64
import logging
import json
import socket
import os
import sys
# 命令行交互
import subprocess

from hashlib import sha1
from config import *

try:
    import locale
    (slocale, sencodstr) = locale.getdefaultlocale()
    sysdir=os.path.abspath(os.path.dirname(__file__))
    sys.path.append(os.sep.join((unicode(sysdir,'GB2312'),'locale/'+slocale)))
    importstring = "from message import *"
    exec importstring
except Exception, e:
    print str(e)

def crypt(data, key):
    """RC4 algorithm"""
    x = 0
    box = range(256)
    for i in range(256):
        x = (x + box[i] + ord(key[i % len(key)])) % 256
        box[i], box[x] = box[x], box[i]
    x = y = 0
    out = []
    for char in data:
        x = (x + 1) % 256
        y = (y + box[x]) % 256
        box[x], box[y] = box[y], box[x]
        out.append(chr(ord(char) ^ box[(box[x] + box[y]) % 256]))

    return ''.join(out)

def tencode(data, key, encode=base64.b64encode, salt_length=16):
    """RC4 encryption with random salt and final encoding"""
    salt = ''
    for n in range(salt_length):
        salt += chr(random.randrange(256))
    data = salt + crypt(data, sha1(key + salt).digest())
    if encode:
        data = encode(data)
    return data

def tdecode(data, key, decode=base64.b64decode, salt_length=16):
    """RC4 decryption of encoded data"""
    if decode:
        data = decode(data)
    salt = data[:salt_length]
    return crypt(data[salt_length:], sha1(key + salt).digest())

def vprint(content, vtuple, logger, level=logging.INFO):
    ncontent = content
    global MESSAGEING
    if MESSAGEING.has_key(content):
        ncontent=MESSAGEING[content].decode('utf-8').encode('gbk')

    try:
        if vtuple is not None:
            ncontent = ncontent % vtuple
    except:
        try:
            content % vtuple
        except Exception, e:
            print content + ' ' + str(vtuple)

    if IS_CONSOLE_PRINT == True:
        print ncontent
    if logger is not None:
        if level == logging.INFO:
            logger.info(ncontent)
        if level == logging.WARN:
            logger.warn(ncontent)
        if level == logging.ERROR:
            logger.error(ncontent)
        if level == logging.DEBUG:
            logger.debug(ncontent)

def cmp_dict(src_data,dst_data):
    assert type(src_data) == type(dst_data),"type: '{}' != '{}'".format(type(src_data), type(dst_data))  
    if isinstance(src_data,dict):  
        assert len(src_data) == len(dst_data),"dict len: '{}' != '{}'".format(len(src_data), len(dst_data))  
        for key in src_data:                  
            assert dst_data.has_key(key)      
            cmp_dict(src_data[key],dst_data[key])      
    elif isinstance(src_data,list):                    
        assert len(src_data) == len(dst_data),"list len: '{}' != '{}'".format(len(src_data), len(dst_data))      
        for src_list, dst_list in zip(sorted(src_data), sorted(dst_data)):  
            cmp_dict(src_list, dst_list)  
    else:  
        assert src_data == dst_data,"value '{}' != '{}'".format(src_data, dst_data)  

def cmpare_dict(src_data,dst_data):
    try:
        cmp_dict(src_data, dst_data)
        return True
    except:
        return False

# 生成随机数数组
def random_int_list(start, stop, length):
    random_list = []
    for i in range(0, length):
        while True:
            rv = random.randint(start, stop)
            if rv not in random_list:
                random_list.append(rv)
                break
    return random_list

# 生成随机端口
def random_port(start, stop, length):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    portList = random_int_list(start, stop, length) 
    for port in portList:
        try:
            s.connect((RPYC_WORK_HOST, int(port)))
            s.shutdown(2)
            continue
        except:
            return port
    return None

def tdecode_rpyc_res(data, key):
    result = tdecode(data,key)
    return json.loads(result)

def runcommands(message,logger):
    jsonobj = json.loads(message)
    response={}
    response['code'] = "0"
    response['response'] = []
    for i in range(0, len(jsonobj)):
        ijsonstr = jsonobj[i]['module_oper']
        ijsonstr = tencode(ijsonstr, RPYC_SECRET_KEY);
        vprint('prepare to invoke executor %s' ,(str(ijsonstr)), logger, logging.DEBUG)
        p = subprocess.Popen('python executor.py ' + ijsonstr, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        p.wait()
        sresult= p.stdout.read()
        print 'ss:' + sresult
        result = tdecode(sresult, RPYC_SECRET_KEY)
        dresult = result.decode('utf-8')
        print 'dresult' + dresult
        response['response'].append(result)
        if p.returncode != 0:
            vprint('response error: body: %s \n',(dresult,), logger, logging.ERROR)
            response['code'] = "1"
        else:
            vprint('response: body: %s \n' , (dresult,), logger, logging.ERROR)
            break
    return json.dumps(response);