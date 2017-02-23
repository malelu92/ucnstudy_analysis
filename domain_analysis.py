import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from collections import defaultdict

from model.Base import Base
from model.Device import Device
from model.DeviceAppTraffic import DeviceAppTraffic
from model.DnsReq import DnsReq
from model.Flow import Flow 
from model.User import User
from model.user_devices import user_devices

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from IPython.display import display

#output_notebook()

def main():
    DB='postgresql+psycopg2:///ucnstudy'

    engine = create_engine(DB, echo=False, poolclass=NullPool)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine) 

    # analizing domains used
    #analyse_domains(Session) 
    
    analyse_beg_end_day(Session)
            

def analyse_beg_end_day(Session):
    ses = Session()

    devices = ses.query(Device).distinct().order_by(Device.id)

    for dev in devices:

        sql_end_day = text("SELECT devid, endts FROM flows join (SELECT DATE(endts) as date_entered, MAX(endts) as max_time FROM flows WHERE devid =:d_id GROUP BY date(endts)) AS grp ON grp.max_time = flows.endts order by flows.endts limit 400;").bindparams(d_id = dev.id)
        result_end_day = ses.execute(sql_end_day)

        info = defaultdict(list)
        for row in result_end_day:
            info['devid'].append(str(row[0]))
            info['end'].append(row[1])

        sql_beg_day = text("SELECT devid, startts FROM flows join (SELECT DATE(startts) as date_entered, MIN(startts) as min_time FROM flows WHERE devid =:d_id GROUP BY date(startts)) AS grp ON grp.min_time = flows.startts order by flows.startts limit 400;").bindparams(d_id = dev.id)
        result_beg_day = ses.execute(sql_beg_day)

        for row in result_beg_day:
            info['start'].append(row[1])

        table = pd.DataFrame(info)
        display(table)

    ses.close()

def analyse_domains (Session):

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
            dom = []
            accesses = []
            length = 0
            for domains in domains_accessed:
                #print (domains.query_domain, domains.count)
                dom.append(domains.query_domain)
                accesses.append(domains.count)
                length = length + 1
            x = np.arange(length)
            plt.bar(x, accesses)
            #plt.xticks(x + 0.5, dom, rotation = 90)
            plt.show()

if __name__ == "__main__":
    main()
