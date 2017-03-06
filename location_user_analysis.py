import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt
from matplotlib import dates

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
                    #insert in the correct position
                    cont = 0
                    for dat in info_beg['beg']:
                        if timst.date() > dat.date():
                            cont = cont + 1
                    info_beg['beg'].insert(cont, timst)
                    info_beg['devid'].insert(cont, row[0])
                    info_beg['location'].insert(cont, row[2])
                    #info_beg['beg'].append(timst)
                    #info_beg['devid'].append(row[0])
                    #info_beg['location'].append(row[2])

            #df_beg = pd.DataFrame(info_beg)
            #display(df_beg)
            #df_end = pd.DataFrame(info_end)
            #display(df_end)

            days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}
            info_week_beg = analyze_per_day(info_beg, 'beg', 'devid', 'location', devices_platform, user, days_str)
            info_week_end = analyze_per_day(info_end, 'end', 'devid', 'location', devices_platform, user, days_str)

                #if info_beg['devid']:
                #    if info_beg['devid'][0] == 4:
     
            #if user.username == 'sormain':
            plot_info(info_week_beg, info_week_end, days_str, user)

def analyze_per_day(info, key_beg_end, key_dev, key_loc, devices_platform, user,days_str):

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
    #days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}
    df_allweek = defaultdict(list)
    for name in days_str:
        df_col = defaultdict(list)
        df_col['device'] = info_week[name + ' platform']
        df_col[name+' '+key_beg_end] = info_week[name]
        df_col['location'] = info_week[name + ' location']

        #df_allweek[name] = info_week
        #df_week = pd.DataFrame(df_col)
        #display(df_week)

    return info_week
                                                                                                    

def plot_info (info_week_beg, info_week_end, days_str, user):
    
    #beginning of day
    df_col_beg = defaultdict(list)
    for weekday in days_str:
        cont = 0
        for timst in info_week_beg[weekday]:
            df_col_beg[weekday + 'date'].append(timst.date())
            #df_col['time'].append(timst.time())
            df_col_beg[weekday + 'time'].append(timst.hour+timst.minute/60.0) 
    
    #end of day
    df_col_end = defaultdict(list)
    for weekday in days_str:
        cont = 0
        for timst in info_week_end[weekday]:
            #norm = timst.day() + timst.month()*12
            #df_col_end[weekday + 'date'].append((int(timst.strftime('%s'))/(3600*24))*3600*24)
            df_col_end[weekday + 'date'].append(timst.date())
            df_col_end[weekday + 'time'].append(timst.hour+timst.minute/60.0)

    fig, ((ax1, ax8), (ax2, ax9), (ax3, ax10), (ax4, ax11), (ax5, ax12), (ax6, ax13),(ax7, ax14)) = plt.subplots(nrows = 7, ncols = 2, figsize=(20, 25))

    create_subplot(ax1, df_col_beg, 'Beginning', 'Monday', user)
    create_subplot(ax2, df_col_beg, 'Beginning', 'Tuesday', user)
    create_subplot(ax3, df_col_beg, 'Beginning', 'Wednesday', user)
    create_subplot(ax4, df_col_beg, 'Beginning', 'Thursday', user)
    create_subplot(ax5, df_col_beg, 'Beginning', 'Friday', user)
    create_subplot(ax6, df_col_beg, 'Beginning', 'Saturday', user)
    create_subplot(ax7, df_col_beg, 'Beginning', 'Sunday', user)

    create_subplot(ax8, df_col_end, 'End', 'Monday', user)
    create_subplot(ax9, df_col_end, 'End', 'Tuesday', user)
    create_subplot(ax10, df_col_end, 'End', 'Wednesday', user)
    create_subplot(ax11, df_col_end, 'End', 'Thursday', user)
    create_subplot(ax12, df_col_end, 'End', 'Friday', user)
    create_subplot(ax13, df_col_end, 'End', 'Saturday', user)
    create_subplot(ax14, df_col_end, 'End', 'Sunday', user)

    fig.subplots_adjust(hspace = .8)
    fig.savefig('figs/' + user.username + '-allweek.png')
    plt.close(fig)


def create_subplot(ax, df_col, key_beg_end, weekday, user):
    x = df_col[weekday+'date']
    y = df_col[weekday+'time']
    sns.set_style('darkgrid')
    #ax.set_xticklabels(x, rotation=45, fontsize = 8, minor=False)
    #ax.set_xticklabels(xlabels, fontsize = 7)
    if len(x) > 1:
        hfmt = dates.DateFormatter('%m-%d')
        ax.xaxis.set_major_formatter(hfmt)
    ax.set_title(key_beg_end + ' of day device usage on ' + weekday + ' - User: ' +  user.username)
    ax.set_ylim([0,24])
    ax.set_ylabel('Hour of Day')
    ax.grid(True)
    ax.plot(x, y, 'o')


if __name__ == "__main__":
    main()
