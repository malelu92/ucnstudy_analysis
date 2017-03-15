import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

from collections import defaultdict

from model.Base import Base
from model.Device import Device
from model.HttpReq import HttpReq
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

    get_http_data()

def get_http_data():

    ses = Session()
    users = ses.query(User)

    #tests only first user
    #users2 = []
    #users2.append(users[0])
    #users2.append(users[1])


    http_beg_userdata = defaultdict(list)
    http_end_userdata = defaultdict(list)
    for user in users:
        sql_user_devices = text('select * from user, user_devices where user_devices.user_id =:user').bindparams(user = user.id)
        user_devices = ses.execute(sql_user_devices)

        #will get the starting time and ending times considering all devices
        print ("user: " + str(user.id) + "=======================")
        quantity_dev = 0
        info_week_beg = {}
        info_week_end ={}
        for dev in user_devices:
            sql_beg_day = text('SELECT distinct devid, httpreqs2.ts FROM httpreqs2 join (SELECT DATE(ts) as date_entered, MIN(ts) as min_time \
            FROM httpreqs2 WHERE devid = :d_id and extract (hour from ts) > 3 \
            GROUP BY date(ts)) AS grp ON grp.min_time = httpreqs2.ts order by httpreqs2.ts;').bindparams(d_id = dev.device_id)
            result_beg_day = ses.execute(sql_beg_day)

            sql_end_day = text('SELECT distinct devid, httpreqs2.ts \
            FROM httpreqs2 join \
            (SELECT DATE(ts) as date_entered, MAX(ts) as max_time \
            FROM httpreqs2 WHERE devid = :d_id  \
            GROUP BY date(ts)) AS grp ON grp.max_time = httpreqs2.ts order by httpreqs2.ts;').bindparams(d_id = dev.device_id)
            result_end_day = ses.execute(sql_end_day)

            sql_beg_day_nolimit = text('SELECT distinct devid, httpreqs2.ts \
            FROM httpreqs2 join \
            (SELECT DATE(ts) as date_entered, MIN(ts) as min_time \
            FROM httpreqs2 WHERE devid = :d_id \
            GROUP BY date(ts)) AS grp ON grp.min_time = httpreqs2.ts order by httpreqs2.ts;').bindparams(d_id = dev.device_id)
            result_beg_day_nolimit = ses.execute(sql_beg_day_nolimit)

            devices_result = ses.query(Device).order_by(Device.id)
            devices_platform = {}
            for item in devices_result:
                devices_platform[item.id] = item.platform

            #organize data
            info_end = defaultdict(list)
            for row in result_end_day:
                info_end['devid'].append(str(row[0]))
                info_end['end'].append(row[1])
        
            info_beg = defaultdict(list)
            for row in result_beg_day:
                info_beg['devid'].append(row[0])
                info_beg['beg'].append(row[1])
                                             
            #add days that only have value before 3 am
            for row in result_beg_day_nolimit:
                timst = row[1]
                in_list = False
                for dt in info_beg['beg']:
                    if dt.date() == timst.date():
                        in_list = True
                if in_list == False:
                    #insert in the correct position
                    cont = 0
                    for dat in info_beg['beg']:
                        if timst.date() > dat.date():
                            cont = cont + 1
                    info_beg['beg'].insert(cont, timst)
                    info_beg['devid'].insert(cont, row[0])

            #df_beg = pd.DataFrame(info_beg)
            #display(df_beg)
            #df_end = pd.DataFrame(info_end)
            #display(df_end)
             
            days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}

            info_week_beg[quantity_dev] = analyze_per_day(info_beg, 'beg', devices_platform[dev.device_id], user, days_str)
            info_week_end[quantity_dev] = analyze_per_day(info_end, 'end', devices_platform[dev.device_id], user, days_str)
            quantity_dev = quantity_dev+1

            #scatter plot
            #scatter_plot(info_week_beg, 'beginning', days_str, user, quantity_dev)
            #scatter_plot(info_week_end, 'end', days_str, user, quantity_dev)

        http_beg_userdata[user.username].append(info_week_beg)
        http_end_userdata[user.username].append(info_week_end)

    return http_beg_userdata, http_end_userdata


def analyze_per_day(info, key_beg_end, platform, user, days_str):

    #create table with times for each week day
    info_week = defaultdict(list)
    if (info[key_beg_end]):
        cont = 0
        for timst in info[key_beg_end]:
            day = timst
            weekday = day.strftime('%A')
            info_week[weekday].append(day)
            info_week['platform'].append(platform)
            cont = cont + 1
         
    info_week['user'] = user.username
    #for name in days_str:
    #    df_col = defaultdict(list)
    #    df_col['device'] = str(dev.device_id)
    #    df_col[name+' beg'] = info_week[name]
    #    df_week = pd.DataFrame(df_col)
    #    display(df_week)
            
    return info_week       


def scatter_plot(info_week, key_beg_end, days_str, user, quantity_dev):
    sns.set_style('darkgrid')
    #for each user device make a scatter plot
    for dev in range (0, quantity_dev):
        x = []
        y = []
        platform = 'none'
        if info_week[dev]['platform']:
            platform = info_week[dev]['platform'][0]
            for weekday in days_str:
                timst_list  = info_week[dev][weekday]
                for timst in timst_list:
                    wkday = convert_weekday(weekday)
                    x.append(wkday)
                    y.append(timst.hour+timst.minute/60.0)

            _, num_x = np.unique(x, return_inverse=True)
            plt.title('http table ' + key_beg_end + ' of day usage -  user: ' + user.username + ' device: ' + platform)
            plt.ylabel('Hour of Day')
            plt.ylim((0,24))
            plt.xticks(num_x, x)
            plt.scatter(num_x, y, s=20, c='b', alpha=0.5)
            plt.savefig('figs_scatter_http/' + user.username + '-' + platform + '-' + key_beg_end +  '-allweek.png')
            plt.close()
            #plt.show()



def convert_weekday(weekday):

    if (weekday == 'Monday'):
        return '0Mon'
    elif (weekday == 'Tuesday'):
        return '1Tue'
    elif (weekday == 'Wednesday'):
        return '2Wed'
    elif (weekday == 'Thursday'):
        return '3Thu'
    elif (weekday == 'Friday'):
        return '4Fri'
    elif (weekday == 'Saturday'):
        return '5Sat'
    else:
        return '6Sun'


if __name__ == '__main__':
    main()
