#!/usr/bin/python
import re
import sys
import os
import glob
import subprocess

from model.Base import Base
from model.User import User
from model.Device import Device
from model.HttpReq import HttpReq

from adblockparser import AdblockRules
from sqlalchemy import create_engine    
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

ctypefilters = [
    re.compile('^.*\.js$'),
    re.compile('^.*\.js\?'),
    re.compile('^.*\.css$'),
    re.compile('^.*\.less$'),
    re.compile('^.*\.swf$'),
    re.compile('^.*\.jpg$'),
    re.compile('^.*\.jpeg$'),
    re.compile('^.*\.gif$'),
    re.compile('^.*\.bmp$'),
    re.compile('^.*\.png$'),
    re.compile('^.*\.jpt$'),
    re.compile('^.*\.ogg$'),
    re.compile('^.*\.mp3$'),
    re.compile('^.*\.mp4$'),
    re.compile('^.*\.mpg$'),
    re.compile('^.*\.mpeg$')
]

easylist = []
with open('/home/apietila/work/dataprojects/ucnstudy/data/easylist/easylist.txt') as f:
    for line in f:
        line = line.strip()
        if (line.startswith('!')):
            continue
        easylist.append(line)
with open('/home/apietila/work/dataprojects/ucnstudy/data/easylist/liste_fr.txt') as f:
    for line in f:
        line = line.strip()
        if (line.startswith('!')):
            continue
        easylist.append(line)
easylist_rules = AdblockRules(easylist, use_re2=True, max_mem=512*1024*1024)

easyprivacy = []
with open('/home/apietila/work/dataprojects/ucnstudy/data/easylist/easyprivacy.txt') as f:
    for line in f:
        line = line.strip()
        if (line.startswith('!')):
            continue
        easyprivacy.append(line)
easyprivacy_rules = AdblockRules(easyprivacy, use_re2=True, max_mem=512*1024*1024)

def handlerecord(dbs, r):
    url = r.req_url

    # fix hostview bugs 
    if (url[-1] == '?'):
        url = url[:-1]
        r.req_url = url

    for rexp in ctypefilters:
        r.matches_ctypefilter = (rexp.match(url) is not None)
        if (r.matches_ctypefilter):
            break
    r.matches_easylist = easylist_rules.should_block(url)
    r.matches_easyprivacy = easyprivacy_rules.should_block(url)
    r.is_referer = (dbs.query(HttpReq).filter(HttpReq.devid==r.devid).filter(HttpReq.req_referer==url).count() > 0)

    r.matches_all = (r.matches_ctypefilter and r.matches_easylist and r.matches_easyprivacy)

    dbs.add(r)

if __name__ == "__main__":
    """
    Do basic filtering of HttpReqs. 
    """
    db = sys.argv[1]
    sys.exit(1)
                
    engine = create_engine(db, poolclass=NullPool, echo=False)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)
    dbs = Session()    
    for r in dbs.query(HttpReq).all():
        print 'handle',str(r)
        handlerecord(dbs, r)
    dbs.commit()
    dbs.close()

