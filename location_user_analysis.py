import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

from collections import defaultdict

from model.Base import Base
from model.Device import Device
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
            (SELECT DATE(locations.entertime) as date_entered, MIN(locations.entertime) as min_time\
            FROM locations, user_devices\
            WHERE devid = user_devices.device_id and user_devices.user_id = :user_id and  extract (hour from entertime) > 3\
            GROUP BY date(entertime))\
            AS grp ON grp.min_time = locations.entertime order by locations.entertime;').bindparams(user_id = user.id)
            result_beg_day = ses.execute(sql_beg_day)

            sql_end_day = text('SELECT distinct devid, locations.exittime, name\
            FROM locations join\
            (SELECT DATE(locations.exittime) as date_entered, MAX(locations.exittime) as max_time\
            FROM locations, user_devices\
            WHERE locations.devid = user_devices.device_id and user_devices.user_id = :user_id\
            GROUP BY date(exittime))\
            AS grp ON grp.max_time = locations.exittime order by locations.exittime;').bindparams(user_id = user.id)

            result_end_day = ses.execute(sql_end_day)

            sql_entertime = text('SELECT distinct devid, locations.entertime, name \
            FROM locations join\
            (SELECT DATE(locations.entertime) as date_entered, MIN(locations.entertime) as min_time\
            FROM locations, user_devices\
            WHERE devid = user_devices.device_id and user_devices.user_id = :user_id\
            GROUP BY date(entertime))\
            AS grp ON grp.min_time = locations.entertime order by locations.entertime;').bindparams(user_id = user.id)
            #sql_entertime = text('select devid, entertime from locations where devid =:d_id').bindparams(d_id = dev.device_id)
            result_entertime = ses.execute(sql_entertime)

            devices_result = ses.query(Device).order_by(Device.id)
            devices_platform = {}
            
            for item in devices_result:
                devices_platform[item.id] = item.platform
                
            #print (devices_platform)
            #    print (item.id)
            #    print(item.platform)

            info_end = defaultdict(list)
            for row in result_end_day:
                info_end['devid'].append(row[0])
                info_end['end'].append(row[1])
                info_end['location'].append(row[2])

            info_beg = defaultdict(list)
            for row in result_beg_day:
                info_beg['devid'].append(row[0])
                info_beg['beg'].append(row[1])
                info_beg['location'].append(row[2])
            
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
            
            #df_beg = pd.DataFrame(info_beg)
            #display(df_beg)
            #df_end = pd.DataFrame(info_end)
            #display(df_end)

            analyze_per_day(info_beg, 'beg', 'devid', 'location', devices_platform, user)
            analyze_per_day(info_end, 'end', 'devid', 'location', devices_platform, user)

def analyze_per_day(info, key_beg_end, key_dev, key_loc, devices_platform, user):

    #create table with times for each week day
    info_week = defaultdict(list)
    if (info[key_beg_end]):
        cont = 0
        for timst in info[key_beg_end]:
            day = timst
            weekday = day.strftime('%A')
            info_week[weekday].append(day)
            info_week[weekday + ' location'].append(info[key_loc][cont])
            info_week[weekday + ' platform'].append(devices_platform[info[key_dev][cont]])
            cont = cont + 1


    #print('Device platform: ' + platform)
    days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}
    for name in days_str:
        df_col = defaultdict(list)
        df_col['device'] = info_week[name + ' platform']
        df_col[name+' '+key_beg_end] = info_week[name]
        df_col['location'] = info_week[name + ' location']

        #df_week = pd.DataFrame(df_col)
        #display(df_week)

        #comecar so com o primeiro usuario
        if info[key_dev]:
            if info[key_dev][0] == 6:
                plot_info(info_week, name, key_beg_end, user)



def plot_info (info_week, weekday, key_beg_end, user):

    #print(info_week[weekday])
    df_col = defaultdict(list)
    cont = 0
    for timst in info_week[weekday]:
        df_col['date'].append(timst.date())
        #df_col['time'].append(timst.time())
        df_col['time'].append(timst.hour+timst.minute/60.0) 
       

    #x = np.arange(len(info_week))

    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    ax.set_title('Initial device usage on ' +  weekday + ' - User: ' +  user.username)
    ax.set_ylim([0,24])
    ax.set_ylabel('Hour of Day') 
    ax.grid(True)
    ax.plot(df_col['date'], df_col['time'])
    fig.savefig('figs/' + user.username + '-' + key_beg_end + str(weekday) + '.png')
    plt.close(fig)
    #plt.show()

if __name__ == "__main__":
    main()
