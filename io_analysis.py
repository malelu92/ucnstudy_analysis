#import numpy as np
import pandas as pd
#import seaborn as sns

import matplotlib.pyplot as plt

from collections import defaultdict

from model_io.Base import Base
from model_io.Devices import Devices
from model_io.Io import Io
#from model.User import User
#from model.user_devices import user_devices

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from IPython.display import display

DB='postgresql+psycopg2:///ucnstudy_hostview_data'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

def get_io_data():

    ses = Session()
    devices = ses.query(Devices)

    for device in devices:
        #select only users from ucnstudy
        if device.id == 5 or device.id == 6 or device.id == 8 or device.id == 11 or device.id == 12:
            print (device.device_id + '===============')

            sql_beg_day = text('SELECT distinct session_id, io.logged_at \
            FROM io join \
            (SELECT DATE(logged_at) as date_entered, MIN(logged_at) as min_time \
            FROM io \
            WHERE session_id =:dev_id and extract (hour from logged_at) > 3 \
            GROUP BY date(logged_at)) AS grp ON grp.min_time = io.logged_at order by io.logged_at;').bindparams(dev_id = device.id)
            result_beg_day = ses.execute(sql_beg_day)

            sql_end_day = text('SELECT distinct session_id, io.logged_at \
            FROM io join \
            (SELECT DATE(logged_at) as date_entered, MAX(logged_at) as max_time \
            FROM io \
            WHERE session_id =:dev_id \
            GROUP BY date(logged_at)) AS grp ON grp.max_time = io.logged_at order by io.logged_at;').bindparams(dev_id = device.id)
            result_end_day = ses.execute(sql_end_day)

            sql_beg_day_nolimit = text('SELECT distinct session_id, io.logged_at \
            FROM io join \
            (SELECT DATE(logged_at) as date_entered, MIN(logged_at) as min_time \
            FROM io \
            WHERE session_id =:dev_id \
            GROUP BY date(logged_at)) AS grp ON grp.min_time = io.logged_at order by io.logged_at;').bindparams(dev_id= device.id)
            result_beg_day_nolimit = ses.execute(sql_beg_day_nolimit)

            #organize data
            info_end = defaultdict(list)
            for row in result_end_day:
                info_end['devid'].append(row[0])
                info_end['end'].append(row[1])


            info_beg = defaultdict(list)
            for row in result_beg_day:
                info_beg['devid'].append(row[0])
                info_beg['start'].append(row[1])
                

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

            df_beg = pd.DataFrame(info_beg)
            display(df_beg)
            df_end = pd.DataFrame(info_end)
            display(df_end)


if __name__ == "__main__":
    get_io_data()
