#!/usr/bin/python
import re
import sys
import os
import glob
import subprocess

from model.Base import Base
from model.User import User
from model.Device import Device
from model.DnsReq import DnsReq

from adblockparser import AdblockRules
from sqlalchemy import create_engine    
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

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

blacklist = [
    re.compile('.*gsp1\.apple\.com'),
    re.compile('.*init-p01st\.push\.apple\.com'),
    re.compile('.*doubleclick\.net'),
    re.compile('.*smartadserver\.com'),
    re.compile('.*pixel\.quantserve\.com'),
    re.compile('.*static\.ess\.apple\.com'),
    re.compile('.*dmd\.metaservices\.microsoft\.com'),
    re.compile('.*windowsupdate\.com')
]

def match(domain):
    matches_easylist = False
    matches_easyprivacy = False
    matches_urlblacklist = False

    matches_easylist = (easylist_rules.should_block('https://%s'%domain) or easylist_rules.should_block('http://%s'%domain))
    matches_easyprivacy = (easyprivacy_rules.should_block('https://%s'%domain) or easyprivacy_rules.should_block('http://%s'%domain))

    for rexp in blacklist:
        matches_blacklist = (rexp.match(domain) is not None)
        if (matches_blacklist):
            break

    return (matches_easylist, matches_easyprivacy, matches_blacklist)


if __name__ == "__main__":
    """
    Do basic filtering of DnsReqs. 
    """
    db = sys.argv[1]
                
    engine = create_engine(db, poolclass=NullPool, echo=False)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)
    
    dbs = Session()    
    for r in dbs.query(DnsReq).filter(DnsReq.query!='').all():
        (matches_easylist, matches_easyprivacy, matches_blacklist) = match(r.query)
        r.matches_easylist = matches_easylist
        r.matches_easyprivacy = matches_easyprivacy
        r.matches_blacklist = matches_blacklist
        r.user_req = (not (matches_easylist or matches_easyprivacy or matches_blacklist))
        print 'update',str(r)
        dbs.add(r)
    dbs.commit()
    dbs.close()

