#!/usr/bin/python
import sys
import os
import glob
import subprocess

from model.Base import Base
from model.User import User
from model.Device import Device

from sqlalchemy import create_engine    
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

# Note, the original version of this script was lost accidentally, 
# redoing the hostview part as it was missing, should re-add 
# the processvpn too. Basically it was using tracesplit or 
# similar from libtrace-tools and filtering the VPN pcaps for 
# the known device IP address...

def processhostview(d):
    """
    Just move the per device hostview pcaps to right place and convert from zip -> gz for tshark.
    """
    for pcap in glob.glob(d.hostviewdata+'/*/*pcap*.zip'):
        tmpfile = '/tmp/' + os.path.basename(pcap).replace('.zip','')        
        if (os.path.exists(tmpfile)):
            os.unlink(tmpfile)
            
        # unzip to tmp location
        subprocess.call(["7z","e","-o/tmp",pcap])
        if (not os.path.exists(tmpfile)):
            continue
            
        dstfile = d.netdata + '/' + os.path.basename(tmpfile) + '.gz'
        if (os.path.exists(dstfile)):
            os.unlink(dstfile)
            
        # convert to the dev data location
        print 'process',tmpfile,'to',dstfile
        subprocess.call(["tracesplit","pcapfile:"+tmpfile,"pcapfile:"+dstfile])
        os.unlink(tmpfile)

def processvpn(d):
    """
    TODO
    """
    return

if __name__ == "__main__":
    """
    Extract per device pcaps to <exp>/devdata/ for easier processing.
    """
    db = sys.argv[1]
                
    engine = create_engine(db, poolclass=NullPool, echo=False)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)

    dbs = Session()    
    if (len(sys.argv)==3):
        d = dbs.query(Device).filter(Device.login==sys.argv[2]).one()
        print 'handle',str(d)
        if (d.hostviewdata!=None):
            processhostview(d)
    else:
        # do all
        for d in dbs.query(Device).all():
            print 'handle',str(d)
            if (d.hostviewdata!=None):
                processhostview(d)
    dbs.close()
