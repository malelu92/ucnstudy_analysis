import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from collections import defaultdict

from model_io.Base import Base
from model_io.Devices import Devices
from model_io.Activities import Activities

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from IPython.display import display

DB='postgresql+psycopg2:///ucnstudy_hostview_data'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)


def get_activities_data():

    ses = Session()
    devices = ses.query(Devices)

    act_beg_userdata = defaultdict(list)
    act_end_userdata = defaultdict(list)
    for device in devices:
        #select only users from ucnstudy
        if device.id == 5 or device.id == 6 or device.id == 8 or device.id == 11 or device.id == 12:
            print (device.device_id + '===============')

            sql_beg_day = text('SELECT distinct session_id, activities.logged_at \
            FROM activities join \
            (SELECT DATE(logged_at) as date_entered, MIN(logged_at) as min_time \
            FROM activities \
            WHERE session_id =:dev_id  and fullscreen = 1 and extract (hour from logged_at) > 3 \
            GROUP BY date(logged_at)) AS grp ON grp.min_time = activities.logged_at \
            order by activities.logged_at;').bindparams(dev_id = device.id)
            result_beg_day = ses.execute(sql_beg_day)

            sql_end_day = text('SELECT distinct session_id, activities.finished_at \
            FROM activities join \
            (SELECT DATE(finished_at) as date_entered, MAX(finished_at) as max_time \
            FROM activities \
            WHERE session_id =:dev_id and fullscreen = 1 \
            GROUP BY date(finished_at)) AS grp ON grp.max_time = activities.finished_at \
            order by activities.finished_at;').bindparams(dev_id = device.id)
            result_end_day = ses.execute(sql_end_day)

            sql_beg_day_nolimit = text('SELECT distinct session_id, activities.logged_at \
            FROM activities join \
            (SELECT DATE(logged_at) as date_entered, MIN(logged_at) as min_time \
            FROM activities \
            WHERE session_id =:dev_id  and fullscreen = 1 \
            GROUP BY date(logged_at)) AS grp ON grp.min_time = activities.logged_at \
            order by activities.logged_at;').bindparams(dev_id =device.id)
            result_beg_day_nolimit = ses.execute(sql_beg_day_nolimit)

            #organize data
            info_end = defaultdict(list)
            for row in result_end_day:
                info_end['devid'].append(row[0])
                info_end['ts_end'].append(row[1])

            info_beg = defaultdict(list)
            for row in result_beg_day:
                info_beg['devid'].append(row[0])
                info_beg['ts_start'].append(row[1])

            #add days that only have value before 3 am
            for row in result_beg_day_nolimit:
                timst = row[1]
                in_list = False
                for dt in info_beg['ts_start']:
                    if dt.date() == timst.date():
                        in_list = True
                if in_list == False:
                    #insert in the correct position
                    cont = 0
                    for dat in info_beg['ts_start']:
                        if timst.date() > dat.date():
                            cont = cont + 1
                    info_beg['ts_start'].insert(cont, timst)
                    info_beg['devid'].insert(cont, row[0])
                

            #df_beg = pd.DataFrame(info_beg)
            #display(df_beg)
            #df_end = pd.DataFrame(info_end)
            #display(df_end)

            days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}

            info_week_beg = analyze_per_day(info_beg, 'ts_start', device.device_id, days_str)
            info_week_end = analyze_per_day(info_end, 'ts_end', device.device_id, days_str)

            #scatter plot
            #scatter_plot(info_week_beg, 'beginning', days_str)
            #scatter_plot(info_week_end, 'end', days_str)

            act_beg_userdata[device.device_id].append(info_week_beg)
            act_end_userdata[device.device_id].append(info_week_end)

    return act_beg_userdata, act_end_userdata

def analyze_per_day(info, key_beg_end, user, days_str):

    #create table with times for each week day
    info_week = defaultdict(list)
    if (info[key_beg_end]):
        for timst in info[key_beg_end]:
            day = timst
            weekday = day.strftime('%A')
            info_week[weekday].append(day)

    info_week['user'] = user
    #for name in days_str:
        #df_col = defaultdict(list)
        #df_col[name + ' ' + key_beg_end] = info_week[name]
        #df_week = pd.DataFrame(df_col)
        #display(df_week)

    return info_week

def scatter_plot(info_week, key_beg_end, days_str):
    sns.set_style('darkgrid')
    x = []
    y = []
    
    for weekday in days_str:
        timst_list  = info_week[weekday]
        for timst in timst_list:
            wkday = convert_weekday(weekday)
            x.append(wkday)
            y.append(timst.hour+timst.minute/60.0)
    
    _, num_x = np.unique(x, return_inverse=True)
    plt.title('activities ' + key_beg_end + ' - user: ' + info_week['user'])
    plt.ylabel('Hour of Day')
    plt.ylim((0,24))
    plt.xticks(num_x, x)
    plt.scatter(num_x, y, s=20, c='b', alpha=0.5)
    plt.savefig('figs_scatter_activity/' + info_week['user'] + '-' + key_beg_end +  '-allweek.png')
    plt.close()

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

if __name__ == "__main__":
    get_activities_data()
