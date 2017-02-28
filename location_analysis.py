import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from collections import defaultdict

from model.Base import Base
from model.Location import Location
from model.User import User
from model.user_devices import user_devices

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from IPython.display import display

def main():
    DB='postgresql+psycopg2:///ucnstudy'

    engine = create_engine(DB, echo=False, poolclass=NullPool)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine)

    analyze_locations_user(Session)


def analyze_locations_user(Session):

    ses = Session()
    users = ses.query(User)

    for user in users:
        sql_user_devices = text('select * from user, user_devices where user_de\
vices.user_id =:user').bindparams(user = user.id)
        user_devices = ses.execute(sql_user_devices)

        #will get the starting time and ending times considering all devices
        print ("user: " + str(user.id) + "=======================")
        for dev in user_devices:

            sql_beg_day = text('SELECT distinct devid, locations.entertime, name \
            FROM locations join\
            (SELECT DATE(entertime) as date_entered, MIN(entertime) as min_time\
            FROM locations\
            WHERE devid = :d_id and extract (hour from entertime) > 3\
            GROUP BY date(entertime))\
            AS grp ON grp.min_time = locations.entertime order by locations.entertime;').bindparams(d_id = dev.device_id)
            result_beg_day = ses.execute(sql_beg_day)

            sql_end_day = text('SELECT distinct devid, locations.exittime, name \
            FROM locations join\
            (SELECT DATE(exittime) as date_entered, MIN(exittime) as min_time\
            FROM locations\
            WHERE devid = :d_id\
            GROUP BY date(exittime))\
            AS grp ON grp.min_time = locations.exittime order by locations.exittime;').bindparams(d_id = dev.device_id)
            result_end_day = ses.execute(sql_end_day)

            sql_entertime = text('SELECT distinct devid, locations.entertime, name \
            FROM locations join\
            (SELECT DATE(entertime) as date_entered, MIN(entertime) as min_time\
            FROM locations\
            WHERE devid = :d_id \
            GROUP BY date(entertime))\
            AS grp ON grp.min_time = locations.entertime order by locations.entertime;').bindparams(d_id = dev.device_id)
            #sql_entertime = text('select devid, entertime from locations where devid =:d_id').bindparams(d_id = dev.device_id)
            result_entertime = ses.execute(sql_entertime)

            info_end = defaultdict(list)
            for row in result_end_day:
                info_end['devid'].append(str(row[0]))
                info_end['end'].append(row[1])
                info_end['location'].append(row[2])

            info_beg = defaultdict(list)
            for row in result_beg_day:
                info_beg['devid'].append(row[0])
                info_beg['beg'].append(row[1])
                info_beg['location'].append(row[2])
            
            print('beg')
            print(len(info_beg['beg']))
            #add days that only have value before 3 am
            for row in result_entertime:
                timst = row[1]
                in_list = False
                for dt in info_beg['beg']:
                    if dt.day == timst.day and dt.month == timst.month and dt.year == timst.year:
                        in_list = True
                if in_list == False:
                    info_beg['beg'].append(timst)
                    info_beg['devid'].append(row[0])
                    info_beg['location'].append(row[2])

            print('beg apos')
            print(len(info_beg['beg']))
            df_beg = pd.DataFrame(info_beg)
            display(df_beg)
            df_end = pd.DataFrame(info_end)
            display(df_end)


if __name__ == "__main__":
    main()
