import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from model.Base import Base
from model.DeviceAppTraffic import DeviceAppTraffic
from model.DnsReq import DnsReq
from model.User import User;
from model.user_devices import user_devices;

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

#output_notebook()

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine) 


# analizing domains used

ses = Session()

users = ses.query(User)

for user in users:
    sql_user_devices = text('select * from user, user_devices where user_devices.user_id =:user').bindparams(user = user.id)
    user_devices = ses.execute(sql_user_devices)

    print ("user: " + str(user.id))
    for devices in user_devices:
        sql_domains_accessed = text('select dnsreqs.query_domain, count(*) from dnsreqs, user_devices where dnsreqs.devid =:devid group by dnsreqs.query_domain').bindparams(devid = devices.device_id)
        domains_accessed = ses.execute(sql_domains_accessed)
        print ("device: " + str(devices.device_id))
        for domains in domains_accessed:
            print (domains.query_domain, domains.count)

