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
    http_week_beg, http_week_end = get_http_data()
    loc_week_beg, loc_week_end = get_locations_data()

    days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}

    #tests only first user
    users2 = []
    users2.append(users[0])
    users2.append(users[1])

    for user in users2:

        fig, ((ax1, ax3, ax5), (ax2, ax4, ax6)) = plt.subplots(nrows = 2, ncols = 3, figsize=(20, 10))
        #fig, (ax3, ax4) = plt.subplots(nrows = 2, ncols = 1, figsize=(20, 25))
        print('scatter user ' + user.username) 
        scatter_plot(ax1, dns_week_beg[user.username][0], 'beg', 'Dns', days_str, user,len(dns_week_beg[user.username]))
        scatter_plot(ax2, dns_week_end[user.username][0], 'end', 'Dns', days_str, user,len(dns_week_end[user.username]))
        scatter_plot(ax3, http_week_beg[user.username][0], 'beg', 'Http', days_str, user,len(http_week_beg[user.username]))
        scatter_plot(ax4, http_week_end[user.username][0], 'end', 'Http', days_str, user,len(http_week_end[user.username]))
        scatter_plot(ax5, loc_week_beg[user.username][0], 'beg', 'Location', days_str, user,len(loc_week_beg[user.username]))
        scatter_plot(ax6, loc_week_end[user.username][0], 'end', 'Location', days_str, user,len(loc_week_end[user.username]))
        #print(dns_week_beg[user.username][0][0]['platform'])


        fig.subplots_adjust(hspace = .8)
        fig.savefig('figs_scatter_activity/' + user.username + '-activity.png')
        plt.close(fig)
    #print (loc_week_beg[0])
    #print (loc_week_end[1])
 
def scatter_plot(ax, info_week, key_beg_end, title, days_str, user, quantity_dev):
    sns.set_style('darkgrid')
    #for each user device make a scatter plot
    for dev in range (0, quantity_dev):
        x = []
        y = []
        #platform = 'none'
        if info_week[dev]['platform']:
            platform = info_week[dev]['platform'][0]
            for weekday in days_str:
                timst_list  = info_week[dev][weekday]
                for timst in timst_list:
                    wkday = convert_weekday(weekday)
                    x.append(wkday)
                    y.append(timst.hour+timst.minute/60.0)
            _, num_x = np.unique(x, return_inverse=True)
            ax.set_title(title + ' table ' + key_beg_end + ' of day usage -  user: ' + user.username + ' device: ' + platform)
            ax.set_ylabel('Hour of Day')
            ax.set_ylim((0,24))
            #ax.set_xlim((-1,7))
            #ax.set_xticks(num_x, x)
            ax.set_xticks(num_x)
            ax.set_xticklabels(x)
            ax.scatter(num_x, y, s=20, c='b', alpha=0.5)
            #plt.savefig('figs_scatter_activity/' + user.username + '-' + platform + '-' + key_beg_end +  '-allweek.png')
            #plt.close()
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





