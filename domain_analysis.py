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

import datautils
import datetime

from IPython.display import display

#output_notebook()

def main():
    DB='postgresql+psycopg2:///ucnstudy'

    engine = create_engine(DB, echo=False, poolclass=NullPool)
    Base.metadata.bind = engine
    Session = sessionmaker(bind=engine) 

    # analizing domains used
    #analyse_domains(Session) 
    
    users_dev_usage = analyse_beg_end_day(Session)

    #for device in users_dev_usage:
    #analyze_week_per_user(Session, users_dev_usage)
            
    #create_graph_beg_end_day(table_info,Session)

    #simple_plot(table_info)

def analyze_week_per_user(Session, users_dev_usage):

    ses = Session()
    users = ses.query(User)
    
    for user in users:

        sql_user_devices = text('select * from user, user_devices where user_devices.user_id =:user').bindparams(user = user.id)
        user_devices = ses.execute(sql_user_devices)

        print (user.id)
        #will get the starting time and ending times considering all devices 
        info = defaultdict(list)
        info['start'] = defaultdict(list)
        info['end'] = defaultdict(list)
        for dev in user_devices:
            for item in users_dev_usage[user.id][dev.device_id]:
                timsts = users_dev_usage[user.id][dev.device_id][item]
                if item[-1] == 't':
                    update_time_lists(timsts, info, 'start')
                elif item[-1] == 'd':
                    update_time_lists(timsts, info, 'end')
                            
                print(item)
                #print (users_dev_usage[user.id][dev.device_id][item])    
    
        df_col = defaultdict(list)
        
        df_col['beg'] = info['start']
        df_col['end'] = info['end']
        df = pd.DataFrame(df_col)
        display(df)


def update_time_lists(timsts, info, key):
    if info[key] == None:
        info[key].append(timsts)
    else:
        #if date is the same and other devices started earlier, get them
        if timsts != None:
            #print ('timst')
            #print(timsts)
            for item in timsts:
                for datetime in item:
                    print('datetime')
                    print (datetime)
                    if datetime.date in info[key]:
                        update_time(datetime, info, key)
                    else:
                        info[key].append(datetime)


def update_time(datetime, info, key):
    for item in info[key]:
        if item.date == datetime.date:
            if item.time > datetime.date:
                item = datetime
   

def simple_plot(table_info):
  #  data = {'x':[], 'y':[], 'label':[]}
    #data['y'].append(table_info['start'])
    #plt.figure(figsize=(12,8))
    #plt.title('test')
    #plt.ylabel('y')
    #plt.scatter(data['x'],data['y'],marker = 'o')

    vals = table_info['start']
    plt.plot([1,2,3,4,5,6,7,8,9,10,11], vals, 'ro')
    plt.show()

def create_graph_beg_end_day(table_info, Session):
    ses = Session()
    uname = 'bridgeman'

    xstart = None
    xend = None

    ts = datautils.utctocc(table_info['start'], 'UK')
    if (xstart == None):
        xstart = ts
        xend = ts
    xstart = xstart if xstart < ts else ts
    xend = xend if xend > ts else ts

    x.append(ts)
    y.append(ts.hour+ts.minute/60.0)

    f, ax1 = plt.subplots(1, 1, figsize=(12, 8))


    ax1.set_title('Beginning and End Times of Daily Usage [user=%s]'%('lalalalalal'))#%(uname))    
    ax1.set_ylim((0,24))
    ax1.set_ylabel('Hour of Day')
    ax1.set_xlim(xstart,xend)
    ax1.legend(loc='best')

    print (table_info['start'][0])
    #ax1.set_xlim((table_info['start']


   # plt.tight_layout()
   # plt.show()

    
    

def analyse_beg_end_day(Session):
    ses = Session()
    
 #   devices = ses.query(Device).distinct().order_by(Device.id)

#    for dev in devices:

    users = ses.query(User)

    users_week_device_info = defaultdict(list)
    for user in users:
        #creates user dictionary
        users_week_device_info[user.id] = {}

        sql_user_devices = text('select * from user, user_devices where user_devices.user_id =:user').bindparams(user = user.id)
        user_devices = ses.execute(sql_user_devices)

        print ("user: " + str(user.id) + "=======================")
        for dev in user_devices:
            #users_week_device_info[user.id[dev.device_id]] = {}

            sql_end_day = text("SELECT distinct devid, flows.endts FROM flows join (SELECT DATE(endts) as date_entered, MAX(endts) as max_time FROM flows WHERE devid =:d_id GROUP BY date(endts)) AS grp ON grp.max_time = flows.endts order by flows.endts;").bindparams(d_id = dev.device_id)
            result_end_day = ses.execute(sql_end_day)

            info = defaultdict(list)
            for row in result_end_day:
                info['devid'].append(str(row[0]))
                info['end'].append(row[1])

            sql_beg_day = text("SELECT distinct devid, flows.startts FROM flows join (SELECT DATE(startts) as date_entered, MIN(startts) as min_time FROM flows WHERE devid =:d_id and extract (hour from startts) > 3 GROUP BY date(startts)) AS grp ON grp.min_time = flows.startts order by flows.startts;").bindparams(d_id = dev.device_id)

            sql_startts = text("select devid, startts from flows where devid = :d_id order by startts").bindparams(d_id = dev.device_id)
            result_beg_day = ses.execute(sql_beg_day)
            result_startts = ses.execute(sql_startts)
            
            for row in result_beg_day:
                info['start'].append(row[1])

            #add days that only have value before 3 am
            for row in result_startts:
                timst = row[1]
                in_list = False
                for date in info['start']:
                    if date.day == timst.day and date.month == timst.month and date.year == timst.year:
                        in_list = True
                if in_list == False:
                    info['start'].append(timst)

            info['start'].sort()
            #df = pd.DataFrame(info)
            #display(df)
        
            
            #users_week_device_info[user.id[dev.device_id]].append(info['start'])
            #users_week_device_info[user.id[dev.device_id]].append(info['end'])
            user_dev = defaultdict(list)
            start_times = str(dev.device_id)+'start'
            user_dev[start_times].append(info['start'])
            end_times = str(dev.device_id)+'end'
            user_dev[end_times].append(info['end'])
            #user_dev['device'] = dev.device_id
            users_week_device_info[user.id][dev.device_id] = user_dev
            #print (users_week_device_info[user.id][dev.device_id][start_times])

            #create table with times for each week day
            #info['start'].sort()
            info_week = defaultdict(list)
            if (info['start']):
                for timst in info['start']:
                    day = timst
                    weekday = day.strftime('%A')
                    key = weekday + ' start'
                    info_week[key].append(day)
                    #print(day, day.strftime('%A'))
        
                for timst in info['end']:
                    day = timst
                    weekday = day.strftime('%A')
                    key = weekday + ' end'
                    info_week[key].append(day)

            days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'}
            for name in days_str:
                df_col = defaultdict(list)
                df_col['device'] = str(dev.device_id)
                df_col[name+' beg'] = info_week[name+' start']
                df_col[name+' end'] = info_week[name+' end']
                df_week = pd.DataFrame(df_col)
                display(df_week)
        
    ses.close()
    #return info
    return users_week_device_info

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
