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

from dnsreqs_analysis import get_dns_data
from httpreqs_analysis import get_http_data
from location_analysis import get_locations_data

from IPython.display import display

DB='postgresql+psycopg2:///ucnstudy'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

def main():
  

    ses = Session()
    users = ses.query(User)

    dns_week_beg, dns_week_end = get_dns_data()
    #http_week_beg, http_week_end = get_http_data()
    #loc_week_beg, loc_week_end = get_locations_data()

    days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}


    for user in users:
        #print('scatter user ' + user.username) 
        scatter_plot(dns_week_beg[user.username][0], 'beg', days_str, user,len(dns_week_beg[user.username]))
        #print(dns_week_beg[user.username][0][0]['platform'])


    #print (loc_week_beg[0])
    #print (loc_week_end[1])
 
def scatter_plot(info_week, key_beg_end, days_str, user, quantity_dev):
    sns.set_style('darkgrid')
    #for each user device make a scatter plot
    print('lala')
    for dev in range (0, quantity_dev):
        x = []
        y = []
        print('lele')
        #platform = 'none'
        if info_week[dev]['platform']:
            platform = info_week[dev]['platform'][0]
            for weekday in days_str:
                timst_list  = info_week[dev][weekday]
                for timst in timst_list:
                    wkday = convert_weekday(weekday)
                    x.append(wkday)
                    y.append(timst.hour+timst.minute/60.0)
            print('lili')
            _, num_x = np.unique(x, return_inverse=True)
            print('lolo')
            plt.title('dns table ' + key_beg_end + ' of day usage -  user: ' + user.username + ' device: ' + platform)
            plt.ylabel('Hour of Day')
            plt.ylim((0,24))
            plt.xticks(num_x, x)
            plt.scatter(num_x, y, s=20, c='b', alpha=0.5)
            plt.savefig('figs_scatter_activity/' + user.username + '-' + platform + '-' + key_beg_end +  '-allweek.png')
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





