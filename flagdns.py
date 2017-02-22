#!/usr/bin/python
import sys
import os

from model.Base import Base
from model.User import User
from model.Device import Device
from model.DnsReq import DnsReq

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

if __name__ == "__main__":
    """
    Flag DNS requests as duplicate when they are followed shortly by a HTTP req.
    
    shortly : 60s
    """
    db = sys.argv[1]
                
    engine = create_engine(db, poolclass=NullPool, echo=False)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)    
    dbs = Session()    
    
    q = """
    select d.id from dnsreqs as d inner join httpreqs as h on d.devid = h.devid and d.query = h.req_url_host and h.ts between d.ts and d.ts + interval '60 seconds'"""
    for r in dbs.execute(text(q)):
        update = """update dnsreqs set duplicate = 't' where id=%d"""%r[0]
        dbs.execute(update)
    dbs.commit()
    dbs.close()