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

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

def main():

    get_locations_data()

def get_locations_data ():

    ses = Session()
    users = ses.query(User)

    #tests only first user
    #users2 = []
    #users2.append(users[0])
    #users2.append(users[1])

    loc_beg_userdata = defaultdict(list)
    loc_end_userdata = defaultdict(list)
    for user in users:
        sql_user_devices = text('select * from user, user_devices where user_de\
vices.user_id =:user').bindparams(user = user.id)
        user_devices = ses.execute(sql_user_devices)

        #will get the starting time and ending times considering all devices
        print ("user: " + str(user.id) + "=======================")
        quantity_dev = 0
        info_week_beg = {}
        info_week_end ={}

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
            (SELECT DATE(exittime) as date_entered, MAX(exittime) as max_time\
            FROM locations\
            WHERE devid = :d_id\
            GROUP BY date(exittime))\
            AS grp ON grp.max_time = locations.exittime order by locations.exittime;').bindparams(d_id = dev.device_id)
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

            devices_result = ses.query(Device).order_by(Device.id)
            devices_platform = {}
            for item in devices_result:
                devices_platform[item.id] = item.platform

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
        
            info_week_beg[quantity_dev] = analyze_per_day(info_beg, 'beg', 'location', devices_platform[dev.device_id], user, days_str)
            info_week_end[quantity_dev] = analyze_per_day(info_end, 'end',  'location', devices_platform[dev.device_id], user, days_str)
            quantity_dev = quantity_dev+1
        

        loc_beg_userdata[user.username].append(info_week_beg)
        loc_end_userdata[user.username].append(info_week_end)
    return loc_beg_userdata, loc_end_userdata
    #return loc_beg_userdata, loc_end_userdata
        #plot per week
        #plot_info(info_week_beg, info_week_end, days_str, user, quantity_dev)

        #scatter plot
        #scatter_plot_location(info_week_beg, 'beginning', days_str, user, quantity_dev)
        #scatter_plot_location(info_week_end, 'end', days_str, user, quantity_dev)



def analyze_per_day(info, key_beg_end, key_loc, platform, user, days_str):

    #create table with times for each week day
    info_week = defaultdict(list)
    if (info[key_beg_end]):
        cont = 0
        for timst in info[key_beg_end]:
            day = timst
            weekday = day.strftime('%A')
            info_week[weekday].append(day)
            info_week[weekday + ' location'].append(info[key_loc][cont])
            info_week['platform'].append(platform)
            cont = cont + 1

    info_week['user'] = user.username
    #df_allweek = defaultdict(list)
    #for name in days_str:
     #   df_col = defaultdict(list)
      #  df_col['device'] = platform[0]
       # df_col[name+' '+key_beg_end] = info_week[name]
        #df_col['location'] = info_week[name + ' location']

        #df_week = pd.DataFrame(df_col)
        #display(df_week)
        
    return info_week

def plot_info (info_week_beg, info_week_end, days_str, user, quantity_dev):

    df_all_dev_beg = {}
    df_all_dev_end = {}

    #for each user device
    for i in range (0, quantity_dev):
        #beginning of day

        df_col_beg = defaultdict(list)
        if info_week_beg[i]['platform']:
            df_col_beg['platform'] = info_week_beg[i]['platform'][0]

        for weekday in days_str:
            cont = 0
            for timst in info_week_beg[i][weekday]:
                df_col_beg[weekday + 'date'].append(timst.date())
                df_col_beg[weekday + 'time'].append(timst.hour+timst.minute/60.0)

    
        #end of day
        df_col_end = defaultdict(list)
        if info_week_end[i]['platform']:
            df_col_end['platform'] = info_week_end[i]['platform'][0]

        for weekday in days_str:
            cont = 0
            for timst in info_week_end[i][weekday]:
                df_col_end[weekday + 'date'].append(timst.date())
                df_col_end[weekday + 'time'].append(timst.hour+timst.minute/60.0)

        df_all_dev_beg[i] = df_col_beg
        df_all_dev_end[i] = df_col_end

    fig, ((ax1, ax8), (ax2, ax9), (ax3, ax10), (ax4, ax11), (ax5, ax12), (ax6, ax13),(ax7, ax14)) = plt.subplots(nrows = 7, ncols = 2, figsize=(20, 25))

    create_subplot(ax1, df_all_dev_beg, 'Beginning', 'Monday', user)
    create_subplot(ax2, df_all_dev_beg, 'Beginning', 'Tuesday', user)
    create_subplot(ax3, df_all_dev_beg, 'Beginning', 'Wednesday', user)
    create_subplot(ax4, df_all_dev_beg, 'Beginning', 'Thursday', user)
    create_subplot(ax5, df_all_dev_beg, 'Beginning', 'Friday', user)
    create_subplot(ax6, df_all_dev_beg, 'Beginning', 'Saturday', user)
    create_subplot(ax7, df_all_dev_beg, 'Beginning', 'Sunday', user)

    create_subplot(ax8, df_all_dev_end, 'End', 'Monday', user)
    create_subplot(ax9, df_all_dev_end, 'End', 'Tuesday', user)
    create_subplot(ax10, df_all_dev_end, 'End', 'Wednesday', user)
    create_subplot(ax11, df_all_dev_end, 'End', 'Thursday', user)
    create_subplot(ax12, df_all_dev_end, 'End', 'Friday', user)
    create_subplot(ax13, df_all_dev_end, 'End', 'Saturday', user)
    create_subplot(ax14, df_all_dev_end, 'End', 'Sunday', user)

    fig.subplots_adjust(hspace = .8)
    fig.savefig('figs2/' + user.username + '-allweek.png')
    plt.close(fig)


def create_subplot(ax, df_all_dev, key_beg_end, weekday, user):
    sns.set_style('darkgrid')

    ax.set_title(key_beg_end + ' of day usage on ' + weekday + ' - User: ' +  user.username)
    ax.set_ylim([0,24])
    ax.set_ylabel('Hour of Day')
    ax.grid(True)

    for dev in range (0, len(df_all_dev)):
        x = df_all_dev[dev][weekday+'date']
        y = df_all_dev[dev][weekday+'time']
        platform = df_all_dev[dev]['platform']

        if len(x) > 1:
            hfmt = dates.DateFormatter('%m-%d')
            ax.xaxis.set_major_formatter(hfmt)
        
        if dev == 0:
            color = 'ro'
        elif dev == 1:
            color = 'bo'
        else:
            color = 'go'

        if platform:
            ax.plot(x, y, color, label=platform)
            ax.legend(loc='best', frameon=True)
     

def scatter_plot(info_week, key_beg_end, days_str, user, quantity_dev):

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
                    if (weekday == 'Monday'): 
                        wkday = '0Mon'
                    elif (weekday == 'Tuesday'): 
                        wkday = '1Tue'
                    elif (weekday == 'Wednesday'):
                        wkday = '2Wed'
                    elif (weekday == 'Thursday'):
                        wkday = '3Thu'
                    elif (weekday == 'Friday'): 
                        wkday = '4Fri'
                    elif (weekday == 'Saturday'):
                        wkday = '5Sat'
                    elif (weekday == 'Sunday'):
                        wkday = '6Sun'

                    x.append(wkday)
                    y.append(timst.hour+timst.minute/60.0)
         
            _, num_x = np.unique(x, return_inverse=True)
            plt.title('Location table ' + key_beg_end + ' of day usage -  user: ' + user.username + ' device: ' + platform)    
            plt.ylabel('Hour of Day')
            plt.ylim((0,24))
            plt.xticks(num_x, x)
            plt.scatter(num_x, y, s=20, c='b', alpha=0.5)
            plt.savefig('figs_scatter_loc/' + user.username + '-' + platform + '-' + key_beg_end +  '-allweek.png')
            plt.close()
            #plt.show()
        
def scatter_plot_location(info_week, key_beg_end, days_str, user, quantity_dev):

    #for each user device make a scatter plot
    for dev in range (0, quantity_dev):
        x = []
        y = []
        if info_week[dev]['platform']:
            platform = info_week[dev]['platform'][0]
            for weekday in days_str:
                timst_list  = info_week[dev][weekday]
                cont = 0
                for timst in timst_list:
                    x.append(info_week[dev][weekday+ ' location'][cont])
                    y.append(timst.hour+timst.minute/60.0)

            _, num_x = np.unique(x, return_inverse=True)
            plt.title('Location table ' + key_beg_end + ' of day usage -  user: ' + user.username + ' device: ' + platform)
            plt.ylabel('Hour of Day')
            plt.ylim((0,24))
            plt.xticks(num_x, x, rotation=35)
            plt.subplots_adjust(bottom = 0.25)
            plt.scatter(num_x, y, s=20, c='b', alpha=0.5)
            plt.savefig('figs_scatter_named_loc/' + user.username + '-' + platform + '-' + key_beg_end +  '-allweek.png')
            plt.close()
            #plt.show()



if __name__ == "__main__":
    main()
