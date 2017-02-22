#!/usr/bin/python
import sys
import os
import re

from model.Base import Base
from model.User import User
from model.Device import Device
from model.DnsReq import DnsReq
from model.HttpReq import HttpReq

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

if __name__ == "__main__":
    """
    Update DnsReq and HttpReq tables with TLD and domain break down of
    the accessed URLs.
    """
    db = sys.argv[1]
                
    engine = create_engine(db, poolclass=NullPool, echo=False)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)    
    dbs = Session()    

    def gettld(host):
        """Get the top-level domain."""        
        if (re.match('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',host)!=None):
            return None
        tmp = host.split('.')
        if (len(tmp)<=1 or len(tmp[-1])>8):
            return None
        else:
            return tmp[-1]

    def getd(host):
        """Get the first level subdomain.tld"""
        if (re.match('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',host)!=None):
            return None
        tmp = host.split('.')
        if (len(tmp)<=1):
            return None
        elif (len(tmp)>=3 and len(tmp[-2])==2 and tmp[-1]=='uk'):
            # co.uk, ac.uk etc
            return ".".join(tmp[-3:])
        else:
            return ".".join(tmp[-2:])

    print 'handle dns'
    for r in dbs.query(DnsReq).all():
        r.query_tld = gettld(r.query)
        r.query_domain = getd(r.query)
        print r.query,r.query_tld,r.query_domain
        dbs.add(r)
    dbs.commit()

    print 'handle http'
    for r in dbs.query(HttpReq).all():
        r.req_url_tld = gettld(r.req_url_host)
        r.req_url_domain = getd(r.req_url_host)
        dbs.add(r)
    dbs.commit()

    print 'done'
    dbs.close()
