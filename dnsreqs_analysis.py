import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

from collections import defaultdict

from model.Base import Base
from model.Device import Device
from model.DnsReq import DnsReq
from model.User import User
from model.user_devices import user_devices

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from IPython.display import display

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

def main():

    ses = Session()
    users = ses.query(User)

    for user in users:
        sql_user_devices = text('select * from user, user_devices where user_devices.user_id =:user').bindparams(user = user.id)
        user_devices = ses.execute(sql_user_devices)

        #will get the starting time and ending times considering all devices
        print ("user: " + str(user.id) + "=======================")
        quantity_dev = 0
        info_week_beg = {}
        info_week_end ={}
        for dev in user_devices:

            sql_beg_day = text('SELECT distinct devid, dnsreqs.ts, query_domain \
            FROM dnsreqs join \
            (SELECT DATE(ts) as date_entered, MIN(ts) as min_time \
            FROM dnsreqs \
            WHERE devid = :d_id and extract (hour from ts) > 3 GROUP BY date(ts))\
            AS grp ON grp.min_time = dnsreqs.ts order by dnsreqs.ts;').bindparams(d_id = dev.device_id)
            result_beg_day = ses.execute(sql_beg_day)
            
            sql_end_day = text('SELECT distinct devid, dnsreqs.ts, query_domain \
            FROM dnsreqs join \
            (SELECT DATE(ts) as date_entered, MAX(ts) as max_time \
            FROM dnsreqs \
            WHERE devid = :d_id GROUP BY date(ts)) \
            AS grp ON grp.max_time = dnsreqs.ts order by dnsreqs.ts;').bindparams(d_id = dev.device_id)
            result_end_day = ses.execute(sql_end_day)

            sql_beg_day_nolimit = text('SELECT distinct devid, dnsreqs.ts, query_domain \
            FROM dnsreqs join \
            (SELECT DATE(ts) as date_entered, MIN(ts) as min_time \
            FROM dnsreqs \
            WHERE devid = :d_id GROUP BY date(ts))\
            AS grp ON grp.min_time = dnsreqs.ts order by dnsreqs.ts;').bindparams(d_id = dev.device_id)
            result_beg_day_nolimit = ses.execute(sql_beg_day_nolimit)

            devices_result = ses.query(Device).order_by(Device.id)
            devices_platform = {}
            for item in devices_result:
                devices_platform[item.id] = item.platform

            #organize data
            info_end = defaultdict(list)
            for row in result_end_day:
                info_end['devid'].append(row[0])
                info_end['end'].append(row[1])
                info_end['domain'].append(row[2])

            info_beg = defaultdict(list)
            for row in result_beg_day:
                info_beg['devid'].append(row[0])
                info_beg['start'].append(row[1])
                info_beg['domain'].append(row[2])

            #add days that only have value before 3 am
            for row in result_beg_day_nolimit:
                timst = row[1]
                in_list = False
                for dt in info_beg['start']:
                    if dt.date() == timst.date():
                        in_list = True
                if in_list == False:
                    #insert in the correct position
                    cont = 0
                    for dat in info_beg['start']:
                        if timst.date() > dat.date():
                            cont = cont + 1
                    info_beg['start'].insert(cont, timst)
                    info_beg['devid'].insert(cont, row[0])
                    info_beg['domain'].insert(cont, row[2])

            df_beg = pd.DataFrame(info_beg)
            display(df_beg)
            df_end = pd.DataFrame(info_end)
            display(df_end)


if __name__ == '__main__':
    main()
