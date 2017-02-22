#!/usr/bin/python
import sys
import os
import glob
import subprocess
import re
import gzip
import urllib
from datetime import datetime

from model.Base import Base
from model.User import User
from model.Device import Device

from sqlalchemy import create_engine    
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

def process(d):
    # read all squid logs for experiment
    ddir = '/home/ucndata/ucnexp'
    if (d.users[0].country == 'fr'):
        ddir += '/proxy/*.gz'
    else:
        ddir += '_uk/proxy/*.gz'

    # output all squid logs for this device to
    fnout = d.netdata+'/squidlog.csv'
    if (os.path.exists(fnout)):
        os.unlink(fnout)
    fout = open(fnout,'w')

    print d.login,fnout,d.addresses
    count = 0

    # now read all logs
    for fn in glob.glob(ddir):
        print 'process',fn

        with gzip.open(fn, 'r') as f:
            for line in f:
                # input format:

                # [05/May/2015:00:18:08 +0200] 1430777888.525     27 10.2.0.24 TCP_MISS/200 580 GET http://prof.estat.com/m/web/261061204390? - HTTP/1.1 HIER_DIRECT/prof.estat.com image/gif "http://www.canalplus.fr/c-emissions/c-le-supplement/pid6586-l-emission.html?vid=1195847" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.5.17 (KHTML, like Gecko) Version/7.1.5 Safari/537.85.14"

                #  [%tl] %ts.%03tu %6tr %>a %Ss/%03Hs %<st %rm %ru %un HTTP/%rv %Sh/%<A %mt "%{Referer}>h" "%{User-Agent}>h"
                m = re.match("\[.+\]\s+(\d+\.\d+)\s+\d+\s+(\d+\.\d+\.\d+\.\d+)\s+[\w_]+/(\d+)\s+(\d+)\s+(\w+)\s+(.+)\s+.+\s+(HTTP/\d+\.\d+)\s+.+\s+(.+)\s+\"(.+)\"\s+\"(.+)\"",line)
                if (m == None):
                    print line
                    continue

                ts = str(m.group(1))
                srcip = str(m.group(2))
                if (not srcip in d.addresses):
                    continue

                status = str(m.group(3))
                size = str(m.group(4))
                method = str(m.group(5))
                url = str(m.group(6))
                if (url[-1] == '?'):
                    url = url[:-1]
		url = urllib.quote(url)

                protocol = str(m.group(7))
                mimetype = str(m.group(8))
                referer = str(m.group(9))
                if (referer == '-'):
                    referer = ''              
		else: 	 
		    referer = urllib.quote(referer)
                useragent = str(m.group(10))
                if (useragent == '-'):
                    useragent = ''                
		useragent = useragent.replace(';',',')
 
#                print ";".join([ts,status,size,method,url,protocol,mimetype,referer,useragent])
                fout.write(";".join([ts,status,size,method,url,protocol,mimetype,referer,useragent,srcip]))
                fout.write("\n")
                count += 1
        fout.flush()
    fout.close()
    print 'found',count,'reqs' 

if __name__ == "__main__":
    """
    Extract per device squid logs.
    """
    db = sys.argv[1]
                
    engine = create_engine(db, poolclass=NullPool, echo=False)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)

    dbs = Session()    
    if (len(sys.argv)==3):
        # single device
        d = dbs.query(Device).filter(Device.login==sys.argv[2]).one()
        print 'handle',str(d)
        process(d)
    else:
        # do all devices
        for d in dbs.query(Device).all():
            print 'handle',str(d)
            process(d)
    dbs.close()
