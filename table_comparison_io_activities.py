import numpy as np
import pandas as pd
import seaborn as sns

import matplotlib.pyplot as plt

from collections import defaultdict

from model_io.Base import Base
from model_io.Devices import Devices

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from activities_analysis import get_activities_data
from io_analysis import get_io_data

DB='postgresql+psycopg2:///ucnstudy_hostview_data'

engine = create_engine(DB, echo=False, poolclass=NullPool)
Base.metadata.bind = engine
Session = sessionmaker(bind=engine)

def main():

    act_week_beg, act_week_end = get_activities_data()
    io_week_beg, io_week_end = get_io_data()
    ses = Session()
    devices = ses.query(Devices)

    days_str = {'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday', 'Sunday'}
    for device in devices:
        #select only users from ucnstudy
        if device.id == 5 or device.id == 6 or device.id == 8 or device.id == 11 or device.id == 12:
            print ('scatter plot: ' + device.device_id + '===============')
            fig, ((ax1, ax3), (ax2, ax4)) = plt.subplots(nrows = 2, ncols = 2, figsize=(20, 10))
            scatter_plot_io(ax1, io_week_beg[device.device_id][0], 'beg', days_str)
            scatter_plot_io(ax2, io_week_end[device.device_id][0], 'end', days_str)
            scatter_plot_act(ax3, act_week_beg[device.device_id][0], 'beg', days_str)
            scatter_plot_act(ax4, act_week_end[device.device_id][0], 'end', days_str)

            fig.subplots_adjust(hspace = .8)
            fig.savefig('figs_scatter_comparison_io/' + device.device_id + '-usage-comparison.png')
            plt.close(fig)

def scatter_plot_io(ax, info_week, key_beg_end, days_str):
    sns.set_style('darkgrid')
    x = []
    y = []
    color = []
    patch = {'c':'Camera', 'r':'Keyboard', 'g':'Microphone', 'y':'Mouse', 'm':'Speaker'}
    interac_legend = {'r':0, 'c':0, 'g':0, 'y':0, 'm':0}

    for weekday in days_str:
        cont = 0
        timst_list  = info_week[weekday]
        for timst in timst_list:
            wkday = convert_weekday(weekday)
            x.append(wkday)
            y.append(timst.hour+timst.minute/60.0)
            get_interaction_color(color, info_week[weekday+'interaction'][cont], cont, patch)
            cont = cont + 1
    _, num_x = np.unique(x, return_inverse=True)
    ax.set_title('Io ' + key_beg_end + ' - user: ' + info_week['user'])
    ax.set_ylabel('Hour of Day')
    ax.set_ylim((0,24))
    ax.set_xticks(num_x)
    ax.set_xticklabels(x)
    for i in range(0, len(num_x)):
        if interac_legend[color[i]] == 0:
            ax.scatter(num_x[i], y[i], s=20, c=color[i], alpha=0.5, label=patch[color[i]])
            interac_legend[color[i]] = 1
        else:
            ax.scatter(num_x[i], y[i], s=20, c=color[i], alpha=0.5)
    ax.legend(loc = 'best')
    
def scatter_plot_act(ax, info_week, key_beg_end, days_str):
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
    ax.set_title('activities ' + key_beg_end + ' - user: ' + info_week['user'])
    ax.set_ylabel('Hour of Day')
    ax.set_ylim((0,24))
    ax.set_xticks(num_x)
    ax.set_xticklabels(x)
    ax.scatter(num_x, y, s=20, c='b', alpha=0.5)
    

def get_interaction_color(color, interaction, pos, patch):
    if interaction == 0:
        color.append('c')
    elif interaction == 1:
        color.append('r')
    elif interaction == 2:
        color.append('g')
    elif interaction == 3:
        color.append('y')
    else:
        color.append('m')


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
