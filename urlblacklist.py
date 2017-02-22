#!/usr/bin/python
import sys
import os
import re

from model.Base import Base

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

blacklist = [
    'gsp1.apple.com',
    'init-p01st.push.apple.com',
    'ad.doubleclick.net',
    'smartadserver.com',
    'pixel.quantserve.com',
    'static.ess.apple.com/connectivity.txt',
    'go.microsoft.com/fwlink',
    'dmd.metaservices.microsoft.com',
    'windowsupdate.com'
]

if __name__ == "__main__":
    """
    More URL filtering.
    """
    db = sys.argv[1]
                
    engine = create_engine(db, poolclass=NullPool, echo=True)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)

    dbs = Session()    
    
    # set to false by default
    sqlq = "UPDATE httpreqs2 SET matches_urlblacklist = 'f', user_url = 't'"
    dbs.execute(text(sqlq))

    for s in blacklist:
        sqlq = "UPDATE httpreqs2 SET matches_urlblacklist = 't' WHERE req_url LIKE '%"+s+"%'"
        dbs.execute(text(sqlq))
 
    sqlq = "UPDATE httpreqs2 SET user_url = 'f' WHERE matches_ctypefilter='t' OR matches_easylist='t' OR matches_urlblacklist = 't'"
    dbs.execute(text(sqlq))    

    dbs.commit()
    dbs.close()


